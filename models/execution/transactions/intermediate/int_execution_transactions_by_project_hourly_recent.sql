{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(hour, project)',
    unique_key='(hour, project)',
    partition_by='toStartOfDay(hour)',
    tags=['production','execution','transactions','hourly']
  )
}}

WITH tx AS (
  SELECT
    date_trunc('hour', block_timestamp)  AS hour,
    lower(from_address)                  AS from_address,
    lower(to_address)                    AS to_address,
    toFloat64(coalesce(gas_used, 0))     AS gas_used,
    toFloat64(coalesce(gas_price, 0))    AS gas_price
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp >= now() - INTERVAL 2 DAY
    AND from_address IS NOT NULL
    AND success = 1

),
lbl AS (
  SELECT 
    address
    ,project
  FROM {{ ref('int_crawlers_data_labels') }}
),
classified AS (
  SELECT
    t.hour,
    IF(l.project IS NOT NULL, l.project, 'Unknown') AS project,
    COUNT()                                         AS tx_count,
    countDistinct(t.from_address)                   AS active_accounts,
    groupBitmapState(cityHash64(t.from_address))    AS ua_bitmap_state,
    SUM(t.gas_used * t.gas_price) / 1e18            AS fee_native_sum
  FROM tx t
  LEFT JOIN lbl l ON t.to_address = l.address
  GROUP BY t.hour, project
),
px AS (
  SELECT
    date,
    price
  FROM {{ ref('stg_crawlers_data__dune_prices') }}
  WHERE symbol = 'XDAI'
  AND date >= now() - INTERVAL 2 DAY
)
SELECT
  c.hour,
  c.project,
  c.tx_count,
  c.active_accounts,
  c.ua_bitmap_state,
  c.fee_native_sum,
  c.fee_native_sum * coalesce(px.price, 1.0) AS fee_usd_sum
FROM classified c
LEFT JOIN px ON px.date = toDate(c.hour)