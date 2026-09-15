{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set incr_end    = var('incremental_end_date', none) %}

{#
  USD layer over the census balances, kept thin and separate for the same reason the
  transfer-derived pair is split: a price backfill rewrites only this model, never the
  balances underneath it.

  Prices join on (date, upper(symbol)) and are LEFT: balance_usd is null wherever the
  price feed has no row for that token-day, which is the documented contract downstream
  consumers already handle. Whole-month incremental filter, as in the model below it.
#}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (start_month or incr_end) else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','rpc_state_indexer','tokens','balances_daily','microbatch']
  )
}}

WITH balances AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        balance_raw,
        balance
    FROM {{ ref('int_rpc_state_indexer_token_balances_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% elif is_incremental() %}
        AND toStartOfMonth(date) >= (
            SELECT toStartOfMonth(max(date)) FROM {{ this }}
        )
      {% endif %}
),

prices AS (
    SELECT
        p.date AS date,
        p.symbol AS symbol,
        p.price AS price
    FROM {{ ref('int_execution_token_prices_daily') }} AS p
    WHERE p.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(p.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(p.date) <= toDate('{{ end_month }}')
      {% elif is_incremental() %}
        AND toStartOfMonth(p.date) >= (
            SELECT toStartOfMonth(max(date)) FROM {{ this }}
        )
      {% endif %}
)

SELECT
    b.date AS date,
    b.token_address AS token_address,
    b.symbol AS symbol,
    b.token_class AS token_class,
    b.address AS address,
    b.balance_raw AS balance_raw,
    b.balance AS balance,
    b.balance * p.price AS balance_usd
FROM balances AS b
LEFT JOIN prices AS p
    ON p.date = b.date
   AND upper(p.symbol) = upper(b.symbol)
