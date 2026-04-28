

-- One row per user_id_hash — all-time lifetime metrics for production traffic.
-- Full rebuild on every run (materialized=table); lifetime aggregates cannot
-- be computed incrementally without reading all history anyway.

SELECT
    user_id_hash,
    max(is_identified)                                              AS is_identified,
    toDate(min(event_time))                                         AS first_seen_date,
    toDate(max(event_time))                                         AS last_seen_date,
    dateDiff('day', toDate(min(event_time)), toDate(max(event_time))) AS lifespan_days,
    uniqExact(event_date)                                           AS days_active,
    count()                                                         AS total_events,
    uniqExact(event_name)                                           AS distinct_event_types,
    uniqExact(page_path)                                            AS distinct_pages_visited,

    -- feature-specific event counts
    countIf(event_name = 'Login with Passkey')                      AS passkey_login_count,
    countIf(event_name = 'Success - Circles mint')                  AS circles_mint_count,
    countIf(event_name = 'Marketplace Purchase')                    AS marketplace_purchase_count,
    countIf(event_category = 'modal')                               AS modal_open_count,
    countIf(event_name = 'Swap' AND is_autocapture = 1)             AS swap_action_count,

    -- category breakdowns
    countIf(event_category = 'pageview')                            AS pageview_count,
    countIf(event_category = 'action')                              AS action_count,
    countIf(event_category = 'feature')                             AS feature_count

FROM `dbt`.`stg_mixpanel_ga__events`
WHERE is_production = 1
GROUP BY user_id_hash