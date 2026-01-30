{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','supply_holders_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

SELECT
    b.date,
    b.token_address,
    any(b.symbol) AS symbol,
    any(b.token_class) AS token_class,

    sumIf(
        b.balance,
        lower(b.address) != '0x0000000000000000000000000000000000000000'
    ) AS supply,

    toUInt64(
      countDistinctIf(
          b.address,
          b.balance > 0
          AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      )
    ) AS holders
FROM {{ ref('int_execution_tokens_balances_daily') }} b
WHERE b.date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
  {% endif %}
GROUP BY b.date, b.token_address
ORDER BY b.date, b.token_address

