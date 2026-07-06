

-- INTERNAL ONLY — per-user acquisition attribution at user_id_hash grain.
-- Mixpanel event UTM is sparse (~3.6% of events carry utm_campaign, on
-- entry/landing hits only), so each identified user's scattered UTM hits are
-- collapsed into one first-touch and last-touch by ordering on event_time.
--
-- First-touch then PREFERS the Mixpanel profile's set-once initial_utm_*
-- (stg_mixpanel_ga__profiles) where present: it is Mixpanel's authoritative
-- first-touch, covers ~19% more users than the event reconstruction, and can
-- predate our event window. When the profile supplies first-touch, first_touch_ts
-- is set to first_seen_at so the downstream causal gate (touch precedes
-- conversion) still credits it — a set-once first-touch precedes any later
-- conversion by definition. last-touch stays event-derived (profiles carry only
-- first-touch). Carries user_id_hash → never exposed to cerebro-api or MCP.

WITH events AS (
    SELECT
        user_id_hash,
        event_time,
        -- strip leaked query strings from dirty campaign values
        -- (e.g. "social_media_profile?utm_source=ig")
        nullIf(splitByChar('?', utm_campaign)[1], '') AS campaign,
        nullIf(utm_source, '')                        AS source,
        nullIf(utm_medium, '')                        AS medium
    FROM `dbt`.`stg_mixpanel_ga__events`
    WHERE is_production = 1
      AND is_identified = 1
),

ev AS (
    SELECT
        user_id_hash,
        argMinIf(campaign, event_time, campaign IS NOT NULL) AS ev_first_campaign,
        argMaxIf(campaign, event_time, campaign IS NOT NULL) AS ev_last_campaign,
        argMinIf(source,   event_time, source   IS NOT NULL) AS ev_first_source,
        argMaxIf(source,   event_time, source   IS NOT NULL) AS ev_last_source,
        argMinIf(medium,   event_time, medium   IS NOT NULL) AS ev_first_medium,
        argMaxIf(medium,   event_time, medium   IS NOT NULL) AS ev_last_medium,
        minIf(event_time, campaign IS NOT NULL)              AS ev_first_touch_ts,
        maxIf(event_time, campaign IS NOT NULL)              AS ev_last_touch_ts,
        min(event_time)                                      AS first_seen_at
    FROM events
    GROUP BY user_id_hash
),

-- Mixpanel profile set-once first-touch (authoritative; overrides event-derived).
prof AS (
    SELECT
        ga_user_id_hash                                       AS user_id_hash,
        nullIf(splitByChar('?', initial_utm_campaign)[1], '') AS p_campaign,
        nullIf(initial_utm_source, '')                        AS p_source,
        nullIf(initial_utm_medium, '')                        AS p_medium
    FROM `dbt`.`stg_mixpanel_ga__profiles`
)

SELECT
    ev.user_id_hash                                                       AS user_id_hash,
    coalesce(p.p_campaign, ev.ev_first_campaign, 'unknown')               AS first_touch_campaign,
    coalesce(ev.ev_last_campaign, p.p_campaign, 'unknown')                AS last_touch_campaign,
    coalesce(p.p_source, ev.ev_first_source, 'unknown')                   AS first_touch_source,
    coalesce(ev.ev_last_source, p.p_source, 'unknown')                    AS last_touch_source,
    coalesce(p.p_medium, ev.ev_first_medium, 'unknown')                   AS first_touch_medium,
    coalesce(ev.ev_last_medium, p.p_medium, 'unknown')                    AS last_touch_medium,
    -- profile-sourced first-touch is set-once and precedes any conversion:
    -- anchor its ts at first_seen_at so the downstream causal gate credits it.
    if(p.p_campaign IS NOT NULL, ev.first_seen_at, ev.ev_first_touch_ts)  AS first_touch_ts,
    ev.ev_last_touch_ts                                                   AS last_touch_ts,
    ev.first_seen_at                                                      AS first_seen_at
FROM ev
LEFT JOIN prof p ON p.user_id_hash = ev.user_id_hash