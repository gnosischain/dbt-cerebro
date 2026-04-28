{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, current_domain, page_path)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, current_domain, page_path)',
    tags=['production', 'mixpanel_ga']
  )
}}

SELECT
    event_date                      AS date,
    current_domain,
    page_path,
    count()                         AS event_count,
    uniqExact(user_id_hash)         AS unique_users
FROM {{ ref('stg_mixpanel_ga__events') }}
WHERE page_path != ''
  AND event_date < today()
  AND is_production = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('event_date', 'date', true) }}
  {% endif %}
GROUP BY date, current_domain, page_path
