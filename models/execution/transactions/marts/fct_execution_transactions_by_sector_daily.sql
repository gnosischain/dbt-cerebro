{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, sector)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, sector)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','transactions']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH base AS (
  SELECT *
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  WHERE 1=1
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', add_and=True, lookback_days=2) }}
  {% endif %}
)

SELECT
    date,
    sector,
    groupBitmapMerge(ua_bitmap_state)        AS active_accounts,
    sum(tx_count)                            AS txs,
    sum(gas_used_sum)                        AS gas_used_sum,
    round(toFloat64(sum(fee_native_sum)), 6) AS fee_native_sum
FROM base
GROUP BY date, sector
