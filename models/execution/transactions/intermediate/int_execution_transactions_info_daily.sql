{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, transaction_type, success)',
    unique_key='(date, transaction_type, success)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','transactions']
  )
}}

WITH tx AS (
  SELECT
    block_timestamp,
    toDate(block_timestamp)                AS date,
    toString(transaction_type)             AS transaction_type,
    COALESCE(success, 0)                   AS success,
    toFloat64OrZero(value) / 1e18          AS value_native, 
    toFloat64OrZero(gas_used)              AS gas_used,
    toFloat64OrZero(gas_price)             AS gas_price
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp < today()
  {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

agg AS (
  SELECT
    date,
    transaction_type,
    success,
    COUNT()                                                AS n_txs,
    SUM(value_native)                                      AS xdai_value,
    AVG(value_native)                                      AS xdai_value_avg,
    median(value_native)                                   AS xdai_value_median,
    -- gas_used is in "gas units" (not wei) – sum as-is
    SUM(gas_used)                                          AS gas_used,
    CAST(AVG(gas_price / 1e9) AS Int32)                    AS gas_price_avg,     -- Gwei
    CAST(median(gas_price / 1e9) AS Int32)                 AS gas_price_median,  -- Gwei
    SUM(gas_used * gas_price) / 1e18                       AS fee_native_sum
  FROM tx
  GROUP BY date, transaction_type, success
),

px AS (
  SELECT
    price_date,
    anyLast(price_usd) AS price_usd
  FROM {{ ref('stg_crawlers_data__dune_prices') }}
  GROUP BY price_date
)

SELECT
  a.date,
  a.transaction_type,
  a.success,
  a.n_txs,
  a.xdai_value,
  a.xdai_value_avg,
  a.xdai_value_median,
  a.gas_used,
  a.gas_price_avg,
  a.gas_price_median,
  a.fee_native_sum,
  a.fee_native_sum * COALESCE(px.price_usd, 1.0) AS fee_usd_sum
FROM agg a
LEFT JOIN px
  ON px.price_date = a.date