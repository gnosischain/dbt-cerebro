{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT
    date,
    referrer_domain,
    initial_referrer_domain,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_traffic_daily') }}
ORDER BY date, event_count DESC
