{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT
    date,
    event_name,
    event_category,
    event_count,
    unique_users,
    unique_devices,
    autocapture_ratio
FROM {{ ref('int_mixpanel_ga_events_daily') }}
ORDER BY date, event_name
