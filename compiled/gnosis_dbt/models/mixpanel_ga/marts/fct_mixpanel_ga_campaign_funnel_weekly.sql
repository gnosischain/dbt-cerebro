

-- Growth funnel by UTM, weekly — every step in one tidy model so "which
-- campaign attracts users" can be read end-to-end (first-touch attribution):
--
--   signup              first identified Mixpanel appearance
--   card_order_started  Mixpanel 'Order the card' CTA click (seed-driven)
--   card_ordered        /gnosis-pay/kyc page reach (seed-driven page proxy —
--                       the order flow emits no completion event)
--   circles_created     Mixpanel 'Create your Circles' (seed-driven)
--   crc_minted          Mixpanel 'Success - Circles mint' (seed-driven)
--   funded              on-chain first Safe inflow (gpay)
--   first_payment       on-chain first card payment (gpay)
--   starts_referring    on-chain first time as Circles inviter (gnosis_app)
--
-- step_order makes "funnel steps non-increasing" checks easy; not every user
-- passes every step so cross-step comparisons are per-campaign trends, not a
-- strict sequential funnel. K-ANONYMITY: campaigns with < 5 all-time signups
-- are bucketed into '_small_campaigns'. Aggregate-only output.

WITH signups AS (
    SELECT
        user_id_hash,
        toStartOfWeek(toDate(first_seen_at), 1) AS week,
        first_touch_campaign                    AS utm_campaign,
        first_touch_source                      AS utm_source,
        first_touch_medium                      AS utm_medium
    FROM `dbt`.`int_mixpanel_ga_user_acquisition`
),

campaign_sizes AS (
    SELECT utm_campaign, count() AS n
    FROM signups
    GROUP BY utm_campaign
),

steps AS (
    SELECT week, 'signup' AS step, 0 AS step_order, utm_campaign, utm_source, utm_medium
    FROM signups

    UNION ALL

    SELECT
        toStartOfWeek(first_date, 1)            AS week,
        metric                                  AS step,
        multiIf(metric = 'card_order_started', 1,
                metric = 'card_ordered', 2,
                metric = 'circles_created', 3,
                metric = 'crc_minted', 4, 9)    AS step_order,
        first_touch_campaign, first_touch_source, first_touch_medium
    FROM `dbt`.`int_mixpanel_ga_client_first_events`

    UNION ALL

    SELECT
        toStartOfWeek(first_date, 1)            AS week,
        event_type                              AS step,
        if(event_type = 'funded', 5, 6)         AS step_order,
        first_touch_campaign, first_touch_source, first_touch_medium
    FROM `dbt`.`int_mixpanel_ga_gpay_first_events`

    UNION ALL

    SELECT
        toStartOfWeek(first_date, 1)            AS week,
        'starts_referring'                      AS step,
        7                                       AS step_order,
        first_touch_campaign, first_touch_source, first_touch_medium
    FROM `dbt`.`int_mixpanel_ga_gnosis_app_first_events`
    WHERE conversion_kind = 'starts_referring'
)

SELECT
    s.week,
    s.step,
    any(s.step_order)                                          AS step_order,
    if(cs.n >= 5, s.utm_campaign, '_small_campaigns')          AS utm_campaign,
    if(cs.n >= 5, s.utm_source,   '_small_campaigns')          AS utm_source,
    if(cs.n >= 5, s.utm_medium,   '_small_campaigns')          AS utm_medium,
    count()                                                    AS new_users
FROM steps s
LEFT JOIN campaign_sizes cs ON cs.utm_campaign = s.utm_campaign
WHERE s.week < toStartOfWeek(today(), 1)
GROUP BY s.week, s.step, utm_campaign, utm_source, utm_medium
ORDER BY s.week, step_order, utm_campaign