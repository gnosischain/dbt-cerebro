{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cohort_week, weeks_since, utm_campaign)',
    tags=['production', 'mixpanel_ga', 'gpay'],
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

-- Per-campaign retention for Gnosis Pay: of the accounts first funded in
-- cohort_week (per first-touch UTM campaign), how many made >= 1 card Payment
-- N weeks later. Answers "which campaign retains" alongside the acquisition
-- view's "which campaign attracts".
--
-- retained_accounts <= cohort_size; week 0 = payments in the funding week
-- itself (not necessarily 100% — funding and first payment can be weeks
-- apart). K-ANONYMITY: cohorts (cohort_week × campaign) smaller than 5
-- accounts are excluded — tiny campaign cohorts are re-identifiable.
-- Aggregate-only output (no pseudonyms).

WITH cohorts AS (
    SELECT user_pseudonym, cohort_week, utm_campaign, utm_source, utm_medium
    FROM {{ ref('int_mixpanel_ga_gpay_campaign_cohorts') }}
),

cohort_sizes AS (
    SELECT cohort_week, utm_campaign, utm_source, utm_medium,
           count(DISTINCT user_pseudonym) AS cohort_size
    FROM cohorts
    GROUP BY cohort_week, utm_campaign, utm_source, utm_medium
    HAVING cohort_size >= 5
),

weekly_payers AS (
    SELECT DISTINCT
        toStartOfWeek(event_date, 1) AS week,
        user_pseudonym
    FROM {{ ref('int_execution_gpay_user_events_unified') }}
    WHERE event_kind = 'gp.payment'
      AND identity_role = 'initial_owner'
      AND event_date < toStartOfWeek(today(), 1)
),

retained AS (
    SELECT
        c.cohort_week,
        toUInt16((p.week - c.cohort_week) / 7)  AS weeks_since,
        c.utm_campaign,
        c.utm_source,
        c.utm_medium,
        count(DISTINCT c.user_pseudonym)        AS retained_accounts
    FROM cohorts c
    INNER JOIN weekly_payers p
        ON p.user_pseudonym = c.user_pseudonym
       AND p.week >= c.cohort_week
    GROUP BY c.cohort_week, weeks_since, c.utm_campaign, c.utm_source, c.utm_medium
)

SELECT
    r.cohort_week,
    r.weeks_since,
    r.utm_campaign,
    r.utm_source,
    r.utm_medium,
    s.cohort_size,
    r.retained_accounts,
    round(r.retained_accounts / s.cohort_size * 100, 2) AS retention_pct
FROM retained r
INNER JOIN cohort_sizes s
    ON s.cohort_week  = r.cohort_week
   AND s.utm_campaign = r.utm_campaign
   AND s.utm_source   = r.utm_source
   AND s.utm_medium   = r.utm_medium
ORDER BY cohort_week, weeks_since, utm_campaign
