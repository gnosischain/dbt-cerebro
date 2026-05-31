{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}
  )
}}

SELECT
    date,
    user_id_hash,
    event_count,
    distinct_event_types,
    distinct_pages
FROM {{ ref('int_mixpanel_ga_users_daily') }}
ORDER BY date, event_count DESC
