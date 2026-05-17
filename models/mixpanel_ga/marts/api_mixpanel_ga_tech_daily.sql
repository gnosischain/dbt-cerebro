{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT
    date,
    browser,
    os,
    device_type,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_tech_daily') }}
ORDER BY date, event_count DESC
