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

WITH
/* 1) Pre-aggregate to shrink early */
tx_daily_to AS (
  SELECT
    toDate(block_timestamp) AS date,
    lower(to_address)       AS to_address,
    count()                 AS tx_count,
    groupBitmapState(cityHash64(lower(from_address))) AS ua_bitmap_state,
    sum(toFloat64(coalesce(gas_used, 0)))             AS gas_used_sum,
    sum(toFloat64(coalesce(gas_used, 0)) * toFloat64(coalesce(gas_price, 0))) / 1e18 AS fee_native_sum
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp < today()
  {{ apply_monthly_incremental_filter('block_timestamp', 'toDate(block_timestamp)') }}
    AND from_address IS NOT NULL
  GROUP BY date, to_address
),

/* 2) Dedup labels to make join safe & light */
lbl AS (
  SELECT address, anyLast(project) AS project
  FROM (
    SELECT lower(address) AS address, project
    FROM {{ ref('stg_crawlers_data__dune_labels') }}
  )
  GROUP BY address
),

/* 3) Join AFTER pre-agg; merge bitmap states per project */
classified AS (
  SELECT
    t.date,
    ifNull(l.project, 'Unknown')                 AS project,
    sum(t.tx_count)                              AS tx_count,

    /* Correct: returns UInt64 */
    groupBitmapMerge(t.ua_bitmap_state)          AS active_accounts,

    /* Keep merged STATE for downstream if needed */
    groupBitmapMergeState(t.ua_bitmap_state)     AS ua_bitmap_state,

    sum(t.gas_used_sum)                          AS gas_used_sum,
    sum(t.fee_native_sum)                        AS fee_native_sum
  FROM tx_daily_to t
  ANY LEFT JOIN lbl l ON t.to_address = l.address
  GROUP BY t.date, project
),

px AS (
  SELECT
    price_date,
    anyLast(price_usd) AS price_usd
  FROM {{ ref('stg_crawlers_data__dune_prices') }}
  GROUP BY price_date
)

SELECT
  c.date,
  c.project,
  c.tx_count,
  c.active_accounts,
  c.ua_bitmap_state,
  c.gas_used_sum,
  c.fee_native_sum,
  c.fee_native_sum * coalesce(px.price_usd, 1.0) AS fee_usd_sum
FROM classified c
LEFT JOIN px ON px.price_date = c.date
