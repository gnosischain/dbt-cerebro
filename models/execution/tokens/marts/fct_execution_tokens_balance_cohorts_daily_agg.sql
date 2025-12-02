{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, balance_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, balance_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balance_cohorts_daily_agg']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH base AS (

    SELECT
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket,
        sum(holders_in_bucket)   AS holders_in_bucket,
        sum(value_usd_in_bucket) AS value_usd_in_bucket
    FROM {{ ref('fct_execution_tokens_balance_cohorts_daily') }}   -- sharded fact
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        balance_bucket
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    holders_in_bucket,
    value_usd_in_bucket
FROM base
WHERE date < today()