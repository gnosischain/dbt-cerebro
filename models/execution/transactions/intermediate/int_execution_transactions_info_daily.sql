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
    toDate(block_timestamp)             AS date,
    toString(transaction_type)          AS transaction_type,
    coalesce(success, 0)                AS success,
    toFloat64(value) / 1e18             AS value_native,             
    toFloat64(coalesce(gas_used, 0))    AS gas_used,                 
    toFloat64(coalesce(gas_price, 0))   AS gas_price                 
  FROM {{ ref('stg_execution__transactions') }}
  WHERE block_timestamp < today()
  {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

agg_base AS (
  SELECT
    date,
    transaction_type,
    success,
    COUNT()                     AS n_txs,
    SUM(value_native)           AS xdai_value_sum_raw,
    AVG(value_native)           AS xdai_value_avg_raw,
    median(value_native)        AS xdai_value_median_raw,
    SUM(gas_used)               AS gas_used_sum_raw,          -- “gas units”
    AVG(gas_price)              AS gas_price_avg_raw_wei,     -- in wei
    median(gas_price)           AS gas_price_med_raw_wei,     -- in wei
    SUM(gas_used * gas_price)   AS fee_sum_raw_wei            -- in wei
  FROM tx
  GROUP BY date, transaction_type, success
),

agg AS (
  SELECT
    date,
    transaction_type,
    success,
    n_txs,
    xdai_value_sum_raw                       AS xdai_value,
    xdai_value_avg_raw                       AS xdai_value_avg,
    xdai_value_median_raw                    AS xdai_value_median,
    gas_used_sum_raw                         AS gas_used,
    CAST(gas_price_avg_raw_wei / 1e9 AS Int32)   AS gas_price_avg,       -- Gwei
    CAST(gas_price_med_raw_wei / 1e9 AS Int32)   AS gas_price_median,    -- Gwei
    fee_sum_raw_wei / 1e18                   AS fee_native_sum          -- xDai
  FROM agg_base
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
  a.fee_native_sum * coalesce(px.price_usd, 1.0) AS fee_usd_sum
FROM agg a
LEFT JOIN px
  ON px.price_date = a.date