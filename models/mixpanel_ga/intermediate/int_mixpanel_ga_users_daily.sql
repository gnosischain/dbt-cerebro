{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, user_id_hash)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, user_id_hash)',
    tags=['production', 'mixpanel_ga']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    event_date                      AS date,
    user_id_hash,
    count()                         AS event_count,
    uniqExact(event_name)           AS distinct_event_types,
    uniqExact(page_path)            AS distinct_pages,
    min(event_time)                 AS first_event_time,
    max(event_time)                 AS last_event_time,
    max(is_identified)              AS is_identified,
    uniqExact(device_id_hash)       AS unique_devices
FROM {{ ref('stg_mixpanel_ga__events') }}
WHERE event_date < today()
  AND is_production = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('event_date', 'date', true) }}
  {% endif %}
GROUP BY date, user_id_hash
