{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT
    date,
    current_domain,
    page_path,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_pages_daily') }}
ORDER BY date, event_count DESC
