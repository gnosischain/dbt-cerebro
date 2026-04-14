{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balances_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}
{% set price_lookback_days = var('price_lookback_days', 3) %}

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
        {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=price_lookback_days, filters_sql=symbol_sql) }}
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
        {{ apply_monthly_incremental_filter('date', 'date', 'true', lookback_days=price_lookback_days, filters_sql=symbol_sql) }}
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
