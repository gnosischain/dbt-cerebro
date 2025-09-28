{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, project)',
    unique_key='(date, project)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','transactions']
  )
}}

WITH tx AS (
  SELECT
    block_timestamp,
    toDate(block_timestamp)              AS date,
    transaction_hash,
    lower(from_address)                  AS from_address,
    lower(to_address)                    AS to_address,
    toFloat64OrZero(gas_used)            AS gas_used,
    toFloat64OrZero(gas_price)           AS gas_price
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp < today()
  {{ apply_monthly_incremental_filter('block_timestamp', 'date') }}
  AND from_address IS NOT NULL
),

lbl AS (
  SELECT lower(address) AS address, project
  FROM {{ ref('stg_crawlers_data__dune_labels') }}
),

classified AS (
  SELECT
    t.date,
    IF(l.project IS NOT NULL, l.project, 'Unknown')  AS project,
    COUNT()                                          AS tx_count,
    countDistinct(t.from_address)                    AS active_accounts,
    groupBitmapState(cityHash64(t.from_address))     AS ua_bitmap_state,
    SUM(t.gas_used)                                  AS gas_used_sum,
    SUM(t.gas_used * t.gas_price) / 1e18             AS fee_native_sum
  FROM tx t
  LEFT JOIN lbl l
    ON t.to_address = l.address
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
  c.fee_native_sum * COALESCE(px.price_usd, 1.0) AS fee_usd_sum
FROM classified c
LEFT JOIN px
  ON px.price_date = c.date