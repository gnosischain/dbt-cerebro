{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(hour, project)',
    unique_key='(hour, project)',
    partition_by='toStartOfDay(hour)',
    post_hook=["ALTER TABLE {{ this }} DELETE WHERE hour < now() - INTERVAL 2 DAY SETTINGS mutations_sync=1"],
    tags=['production','execution','transactions','hourly']
  )
}}

WITH tx AS (
  SELECT
    date_trunc('hour', block_timestamp) AS hour,
    lower(from_address)                 AS from_address,
    lower(to_address)                   AS to_address,
    toFloat64OrZero(gas_used)           AS gas_used,
    toFloat64OrZero(gas_price)          AS gas_price
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp >= now() - INTERVAL 2 DAY
),
lbl AS (
  SELECT address, project FROM {{ ref('stg_crawlers_data__dune_labels') }}
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
  SELECT price_date, anyLast(price_usd) AS price_usd
  FROM {{ ref('stg_crawlers_data__dune_prices') }}
  GROUP BY price_date
)
SELECT
  c.hour,
  c.project,
  c.tx_count,
  c.active_accounts,
  c.ua_bitmap_state,
  c.fee_native_sum,
  c.fee_native_sum * COALESCE(px.price_usd, 1.0) AS fee_usd_sum
FROM classified c
LEFT JOIN px ON px.price_date = toDate(c.hour)