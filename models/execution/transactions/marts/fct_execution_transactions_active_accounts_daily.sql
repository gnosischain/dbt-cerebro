{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date)',
    tags=['production','execution','transactions']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

SELECT
    date,
    groupBitmapMerge(ua_bitmap_state) AS active_accounts
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE 1=1
{% if start_month and end_month %}
  AND toStartOfMonth(date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(date) <= toDate('{{ end_month }}')
{% else %}
  {{ apply_monthly_incremental_filter('date', 'date', add_and=True, lookback_days=2) }}
{% endif %}
GROUP BY date
