

-- INTERNAL ONLY — one row per Mixpanel user per client-side conversion metric,
-- with the user's UTM attribution attached. Two seed-driven mappings feed it so
-- Growth can add or re-point metrics without a code change:
--   event_name → metric  (seeds/mixpanel_conversion_events.csv)
--   page_path  → metric  (seeds/mixpanel_conversion_pages.csv) — for funnel
--   steps with no custom Mixpanel event (e.g. card_ordered = /gnosis-pay/kyc
--   page reach; the order flow emits no completion event).
-- First occurrence per (user, metric) = the conversion moment.
-- Carries user_id_hash → never exposed to cerebro-api or MCP.

WITH conv_events AS (
    SELECT
        m.metric                AS metric,
        e.user_id_hash          AS user_id_hash,
        min(e.event_time)       AS first_ts
    FROM `dbt`.`stg_mixpanel_ga__events` e
    INNER JOIN `dbt`.`mixpanel_conversion_events` m
        ON m.event_name = e.event_name
    WHERE e.is_production = 1
      AND e.is_identified = 1
    GROUP BY m.metric, e.user_id_hash
),

conv_pages AS (
    SELECT
        p.metric                AS metric,
        e.user_id_hash          AS user_id_hash,
        min(e.event_time)       AS first_ts
    FROM `dbt`.`stg_mixpanel_ga__events` e
    INNER JOIN `dbt`.`mixpanel_conversion_pages` p
        ON p.page_path = e.page_path
    WHERE e.is_production = 1
      AND e.is_identified = 1
    GROUP BY p.metric, e.user_id_hash
),

conv AS (
    SELECT
        metric,
        user_id_hash,
        min(first_ts) AS first_ts
    FROM (
        SELECT metric, user_id_hash, first_ts FROM conv_events
        UNION ALL
        SELECT metric, user_id_hash, first_ts FROM conv_pages
    )
    GROUP BY metric, user_id_hash
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
LEFT JOIN `dbt`.`int_mixpanel_ga_user_acquisition` a
    ON a.user_id_hash = c.user_id_hash