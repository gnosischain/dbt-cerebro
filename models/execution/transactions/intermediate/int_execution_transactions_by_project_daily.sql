{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, project)',
    unique_key='(date, project)',
    partition_by='toStartOfMonth(date)',
    settings={
      'allow_nullable_key': 1
    },
    tags=['production','execution','transactions']
  )
}}

WITH lbl AS (
    SELECT 
      address
      ,project
    FROM {{ ref('int_crawlers_data_labels') }}
),

tx_labeled AS (
  -- map tx -> project first, then aggregate by (date, project)
  SELECT
    toDate(t.block_timestamp)                        AS date,
    coalesce(nullIf(trim(l.project), ''), 'Unknown') AS project,
    lower(t.from_address)                            AS from_address,
    toFloat64(coalesce(t.gas_used, 0))               AS gas_used,
    toFloat64(coalesce(t.gas_price, 0))              AS gas_price
  FROM {{ ref('stg_execution__transactions') }} t
  ANY LEFT JOIN lbl l ON lower(t.to_address) = l.address
  WHERE t.block_timestamp < today()
    AND t.from_address IS NOT NULL
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

agg AS (
  SELECT
    date,
    project,
    count()                                           AS tx_count,
    groupBitmap(cityHash64(from_address))             AS active_accounts,
    groupBitmapState(cityHash64(from_address))        AS ua_bitmap_state,
    sum(gas_used)                                     AS gas_used_sum,
    sum(gas_used * gas_price) / 1e18                  AS fee_native_sum
  FROM tx_labeled
  GROUP BY date, project
),

px AS (
  SELECT
    date,
    price
  FROM {{ ref('stg_crawlers_data__dune_prices') }}
  WHERE symbol = 'XDAI'
  {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
)

SELECT
  a.date                                     AS date,
  a.project                                  AS project,
  a.tx_count                                 AS tx_count,
  a.active_accounts                          AS active_accounts,
  a.ua_bitmap_state                          AS ua_bitmap_state,
  a.gas_used_sum                             AS gas_used_sum,
  a.fee_native_sum                           AS fee_native_sum,
  a.fee_native_sum * coalesce(px.price, 1.0) AS fee_usd_sum
FROM agg a
LEFT JOIN px ON px.date = a.date
