{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, country_code, region)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, country_code, region)',
    tags=['production', 'mixpanel_ga']
  )
}}

SELECT
    event_date                      AS date,
    country_code,
    region,
    count()                         AS event_count,
    uniqExact(user_id_hash)         AS unique_users,
    uniqExact(device_id_hash)       AS unique_devices
FROM {{ ref('stg_mixpanel_ga__events') }}
WHERE country_code != ''
  AND event_date < today()
  AND is_production = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(event_date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(event_date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('event_date', 'date', true) }}
  {% endif %}
GROUP BY date, country_code, region
