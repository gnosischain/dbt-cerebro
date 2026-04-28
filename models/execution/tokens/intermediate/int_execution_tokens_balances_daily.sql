{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set incr_end    = var('incremental_end_date', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}

{#
  incremental_strategy resolves to `append` when either start_month
  (full-refresh batching) OR incremental_end_date (microbatch runner) is set.
  The macros no-overlap branch caps the slice; ReplacingMergeTree dedups
  on (date, token_address, address). Eliminates the ALTER ... DELETE
  mutation that was OOM-ing under CreatingSetsTransform (CH 341).
#}
{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (start_month or incr_end) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_memory_usage = 6000000000",
      "SET max_bytes_before_external_group_by = 2000000000",
      "SET max_bytes_before_external_sort = 2000000000",
      "SET join_algorithm = 'grace_hash'"
    ],
    post_hook=[
      "SET max_memory_usage = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0",
      "SET join_algorithm = 'default'"
    ],
    tags=['production','execution','tokens','balances_daily','microbatch','refill_append']
  )
}}

{#
  Recovery after a prices-source gap is *not* via `price_lookback_days` on
  this model — the delete+insert branch with a wide window OOMs (CH 341).
  Instead the refill script does a per-month append rewrite + OPTIMIZE
  PARTITION FINAL DEDUPLICATE (Phase 1 of refill_after_price_gap.sh).
#}

{% set symbol_sql %}
  {{ symbol_filter('symbol', symbol, 'include') }}
  {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
{% endset %}

WITH balances AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        balance_raw,
        balance
    FROM {{ ref('int_execution_tokens_balances_native_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true', filters_sql=symbol_sql) }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
),

prices AS (
    SELECT
        p.date,
        p.symbol,
        p.price
    FROM {{ ref('int_execution_token_prices_daily') }} p
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true', filters_sql=symbol_sql) }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
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
FROM balances b
LEFT JOIN prices p
  ON p.date = b.date
 AND upper(p.symbol) = upper(b.symbol)
