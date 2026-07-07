{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'gpay', 'granularity:weekly']
  )
}}

-- GROWTH — Gnosis Pay card-spend as WEEKLY TOTALS (no campaign dimension).
-- Summed over campaign from api_mixpanel_ga_gpay_campaign_metrics_weekly. Each GP account
-- carries exactly one first-touch campaign, so the per-campaign partitions are disjoint and
-- active_accounts / payments / USD sum cleanly to the weekly total. avg_payments_per_account
-- is a ratio and is recomputed in the outer query (never summed). Aggregate-only; served via
-- x-api-key. The ratio is computed outside the aggregation to avoid the alias-shadow nested
-- aggregate (sum(payments) AS payments would otherwise resolve inside a second sum()).

SELECT
    week,
    active_accounts,
    payments,
    payment_volume_usd,
    cashback_usd,
    round(payments / nullIf(active_accounts, 0), 2) AS avg_payments_per_account
FROM (
    SELECT
        week,
        sum(active_accounts)    AS active_accounts,
        sum(payments)           AS payments,
        sum(payment_volume_usd) AS payment_volume_usd,
        sum(cashback_usd)       AS cashback_usd
    FROM {{ ref('api_mixpanel_ga_gpay_campaign_metrics_weekly') }}
    GROUP BY week
)
