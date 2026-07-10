{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week)',
    tags=['production', 'mixpanel_ga']
  )
}}

-- Weekly card-order funnel over the signup cohort — users whose first
-- 'Create my profile' event fired that ISO week (the Growth dashboard's
-- "Signups"). card_eligible = cohort users whose Mixpanel profile country is
-- in seeds/card_eligible_countries.csv (EU-27 + NO/CH/GB). Numerators are
-- eligible-scoped conversions landing in the SAME week:
--   card_order_started   'Order the card' CTA click (seed-driven)
--   card_ordered         /gnosis-pay/kyc page reach (seed-driven page proxy)
-- both via int_mixpanel_ga_client_first_events. Rates use the ELIGIBLE
-- denominator, matching the dashboard (e.g. 62/98 started, 44/98 ordered).
-- Users with no profile or blank country count as NOT eligible.
-- Clipped to weeks >= 2026-03-02 ('Create my profile' first appears
-- 2026-02-26); current week excluded. Aggregate-only output.

WITH signups AS (
    SELECT
        user_id_hash,
        toStartOfWeek(min(event_date), 1) AS week
    FROM {{ ref('stg_mixpanel_ga__events') }}
    WHERE is_production = 1
      AND event_name = 'Create my profile'
    GROUP BY user_id_hash
),

eligibility AS (
    SELECT
        user_id_hash,
        if(country_code IN (SELECT country_code FROM {{ ref('card_eligible_countries') }}), 1, 0) AS is_eligible
    FROM {{ ref('int_mixpanel_ga_user_profile') }}
),

conv AS (
    SELECT
        metric,
        user_id_hash,
        toStartOfWeek(first_date, 1) AS week
    FROM {{ ref('int_mixpanel_ga_client_first_events') }}
    WHERE metric IN ('card_order_started', 'card_ordered')
)

SELECT
    s.week                                                  AS week,
    count()                                                 AS signups,
    sum(e.is_eligible)                                      AS card_eligible,
    countIf(e.is_eligible = 1 AND st.week = s.week)         AS eligible_order_started,
    countIf(e.is_eligible = 1 AND od.week = s.week)         AS eligible_card_ordered,
    round(
        countIf(e.is_eligible = 1 AND st.week = s.week)
        / greatest(sum(e.is_eligible), 1),
        4
    )                                                       AS order_started_rate,
    round(
        countIf(e.is_eligible = 1 AND od.week = s.week)
        / greatest(sum(e.is_eligible), 1),
        4
    )                                                       AS card_ordered_rate
FROM signups s
LEFT JOIN eligibility e ON e.user_id_hash = s.user_id_hash
LEFT JOIN conv st ON st.user_id_hash = s.user_id_hash AND st.metric = 'card_order_started'
LEFT JOIN conv od ON od.user_id_hash = s.user_id_hash AND od.metric = 'card_ordered'
WHERE s.week < toStartOfWeek(today(), 1)
  AND s.week >= toDate('2026-03-02')
GROUP BY s.week
ORDER BY s.week
