{{ 
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(hour, project)',
    unique_key='(hour, project)',
    tags=['production','execution','transactions','hourly']
  ) 
}}

WITH lbl AS (
  SELECT address, project, sector
  FROM {{ ref('int_crawlers_data_labels') }}
),

wm AS (
  SELECT toStartOfHour(max(block_timestamp)) AS max_hour
  FROM {{ ref('stg_execution__transactions') }}
  WHERE toStartOfMonth(block_timestamp) >= toStartOfMonth(today() - INTERVAL 1 MONTH)
),

tx AS (
  SELECT
    date_trunc('hour', t.block_timestamp) AS hour,
    lower(t.from_address)                 AS from_address,
    lower(t.to_address)                   AS to_address,
    toFloat64(coalesce(t.gas_used, 0))    AS gas_used,
    toFloat64(coalesce(t.gas_price, 0))   AS gas_price
  FROM {{ ref('stg_execution__transactions') }} t
  CROSS JOIN wm
  WHERE 
    toStartOfMonth(block_timestamp) >= toStartOfMonth(today() - INTERVAL 1 MONTH)
    AND t.block_timestamp >  subtractHours(max_hour, 47)
    AND t.block_timestamp <= max_hour
    AND t.from_address IS NOT NULL
    AND t.success = 1
),

classified AS (
  SELECT
    tx.hour,
    coalesce(nullIf(trim(l.project), ''), 'Unknown') AS project,
    count()                                          AS tx_count,
    countDistinct(tx.from_address)                   AS active_accounts,
    groupBitmapState(cityHash64(tx.from_address))    AS ua_bitmap_state,
    sum(tx.gas_used * tx.gas_price) / 1e18           AS fee_native_sum
  FROM tx
  ANY LEFT JOIN lbl l ON tx.to_address = l.address
  GROUP BY tx.hour, project
),

proj_sector AS (
  SELECT
    project,
    coalesce(nullIf(trim(sector), ''), 'Unknown') AS sector
  FROM (
    SELECT project, anyHeavy(sector) AS sector
    FROM {{ ref('int_crawlers_data_labels') }}
    GROUP BY project
  )
)

SELECT
  c.hour,
  c.project,
  ps.sector,
  c.tx_count,
  c.active_accounts,
  c.ua_bitmap_state,
  c.fee_native_sum
FROM classified c
LEFT JOIN proj_sector ps ON ps.project = c.project