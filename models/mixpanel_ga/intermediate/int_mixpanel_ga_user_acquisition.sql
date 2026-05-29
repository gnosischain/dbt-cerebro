{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_id_hash)',
    tags=['production', 'mixpanel_ga', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}
  )
}}

-- INTERNAL ONLY — per-user acquisition attribution at user_id_hash grain.
-- Mixpanel UTM is sparse (~3.6% of events carry utm_campaign, on entry/landing
-- hits only). This model collapses each identified user's scattered UTM hits
-- into one stable first-touch and last-touch attribution by ordering on
-- event_time. No initial_utm_* super-properties exist in the raw data, so
-- first-touch is derived here rather than read off the event.
-- Carries user_id_hash → never exposed to cerebro-api or MCP.

WITH events AS (
    SELECT
        user_id_hash,
        event_time,
        -- strip leaked query strings from dirty campaign values
        -- (e.g. "social_media_profile?utm_source=ig")
        nullIf(splitByChar('?', utm_campaign)[1], '') AS campaign,
        nullIf(utm_source, '')                        AS source,
        nullIf(utm_medium, '')                        AS medium
    FROM {{ ref('stg_mixpanel_ga__events') }}
    WHERE is_production = 1
      AND is_identified = 1
)

SELECT
    user_id_hash,
    coalesce(argMinIf(campaign, event_time, campaign IS NOT NULL), 'unknown') AS first_touch_campaign,
    coalesce(argMaxIf(campaign, event_time, campaign IS NOT NULL), 'unknown') AS last_touch_campaign,
    coalesce(argMinIf(source,   event_time, source   IS NOT NULL), 'unknown') AS first_touch_source,
    coalesce(argMaxIf(source,   event_time, source   IS NOT NULL), 'unknown') AS last_touch_source,
    coalesce(argMinIf(medium,   event_time, medium   IS NOT NULL), 'unknown') AS first_touch_medium,
    coalesce(argMaxIf(medium,   event_time, medium   IS NOT NULL), 'unknown') AS last_touch_medium,
    min(event_time)                                                           AS first_seen_at
FROM events
GROUP BY user_id_hash
