{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    partition_by='toStartOfMonth(date)',
    order_by='(address, date)',
    unique_key='(address, date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production', 'execution', 'accounts', 'portfolio', 'balances', 'granularity:daily']
  )
}}

-- Thin pass-through over int_execution_account_balance_history_daily.
-- The heavy address × token × date aggregation lives in the int_ model so
-- this layer is cheap to refresh.

SELECT
  address,
  date,
  total_balance_usd,
  tokens_held,
  native_or_wrapped_xdai_balance,
  priced_balance_usd,
  priced_tokens_held
FROM {{ ref('int_execution_account_balance_history_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
  {% endif %}
