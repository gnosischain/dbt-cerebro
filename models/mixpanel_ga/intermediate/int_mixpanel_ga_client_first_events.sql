{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(metric, first_date, user_id_hash)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mixpanel_ga', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}
  )
}}

-- INTERNAL ONLY — one row per Mixpanel user per client-side conversion metric,
-- with the user's UTM attribution attached. The event_name → metric mapping is
-- seed-driven (seeds/mixpanel_conversion_events.csv) so Growth can add or
-- re-point metrics (card_ordered, crc_minted, circles_created, ...) without a
-- code change. First occurrence per (user, metric) = the conversion moment.
-- Carries user_id_hash → never exposed to cerebro-api or MCP.

WITH conv AS (
    SELECT
        m.metric                AS metric,
        e.user_id_hash          AS user_id_hash,
        min(e.event_time)       AS first_ts
    FROM {{ ref('stg_mixpanel_ga__events') }} e
    INNER JOIN {{ ref('mixpanel_conversion_events') }} m
        ON m.event_name = e.event_name
    WHERE e.is_production = 1
      AND e.is_identified = 1
    GROUP BY m.metric, e.user_id_hash
)

SELECT
    c.metric                                      AS metric,
    toDate(c.first_ts)                            AS first_date,
    c.user_id_hash                                AS user_id_hash,
    coalesce(a.first_touch_campaign, 'unknown')   AS first_touch_campaign,
    coalesce(a.last_touch_campaign,  'unknown')   AS last_touch_campaign,
    coalesce(a.first_touch_source,   'unknown')   AS first_touch_source,
    coalesce(a.last_touch_source,    'unknown')   AS last_touch_source,
    coalesce(a.first_touch_medium,   'unknown')   AS first_touch_medium,
    coalesce(a.last_touch_medium,    'unknown')   AS last_touch_medium
FROM conv c
LEFT JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} a
    ON a.user_id_hash = c.user_id_hash
