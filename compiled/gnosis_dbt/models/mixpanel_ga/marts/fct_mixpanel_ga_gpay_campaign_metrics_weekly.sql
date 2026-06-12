

-- Per-campaign weekly engagement/value for Gnosis Pay accounts: of the
-- accounts attributed to each first-touch UTM campaign, their weekly card
-- Payment activity (active accounts, payment count, USD volume, cashback)
-- — the "downstream detail" beyond acquisition counts.
--
-- K-ANONYMITY: campaigns with fewer than 5 attributed funded accounts
-- all-time are bucketed into '_small_campaigns'. Aggregate-only output.

WITH cohorts AS (
    SELECT user_pseudonym, utm_campaign, utm_source, utm_medium
    FROM `dbt`.`int_mixpanel_ga_gpay_campaign_cohorts`
),

campaign_sizes AS (
    SELECT utm_campaign, count(DISTINCT user_pseudonym) AS n
    FROM cohorts
    GROUP BY utm_campaign
),

cohorts_bucketed AS (
    SELECT
        c.user_pseudonym,
        if(s.n >= 5, c.utm_campaign, '_small_campaigns') AS utm_campaign,
        if(s.n >= 5, c.utm_source,   '_small_campaigns') AS utm_source,
        if(s.n >= 5, c.utm_medium,   '_small_campaigns') AS utm_medium
    FROM cohorts c
    INNER JOIN campaign_sizes s ON s.utm_campaign = c.utm_campaign
),

payments AS (
    SELECT
        toStartOfWeek(event_date, 1) AS week,
        user_pseudonym,
        count()                      AS n_payments,
        sum(amount_usd)              AS volume_usd
    FROM `dbt`.`int_execution_gpay_user_events_unified`
    WHERE event_kind = 'gp.payment'
      AND identity_role = 'initial_owner'
      AND event_date < toStartOfWeek(today(), 1)
    GROUP BY week, user_pseudonym
),

cashback AS (
    SELECT
        toStartOfWeek(event_date, 1) AS week,
        user_pseudonym,
        sum(amount_usd)              AS cashback_usd
    FROM `dbt`.`int_execution_gpay_user_events_unified`
    WHERE event_kind = 'gp.cashback_claim'
      AND identity_role = 'initial_owner'
      AND event_date < toStartOfWeek(today(), 1)
    GROUP BY week, user_pseudonym
)

SELECT
    p.week                                                    AS week,
    c.utm_campaign,
    c.utm_source,
    c.utm_medium,
    count(DISTINCT p.user_pseudonym)                          AS active_accounts,
    sum(p.n_payments)                                         AS payments,
    sum(p.volume_usd)                                         AS payment_volume_usd,
    sum(cb.cashback_usd)                                      AS cashback_usd,
    round(sum(p.n_payments) / count(DISTINCT p.user_pseudonym), 2) AS avg_payments_per_account
FROM payments p
INNER JOIN cohorts_bucketed c
    ON c.user_pseudonym = p.user_pseudonym
LEFT JOIN cashback cb
    ON cb.user_pseudonym = p.user_pseudonym
   AND cb.week = p.week
GROUP BY p.week, c.utm_campaign, c.utm_source, c.utm_medium
ORDER BY p.week, c.utm_campaign