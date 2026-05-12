{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_invite_funnel_cohort','granularity:monthly']
  )
}}

-- Cohort by invitation month: how many invitees minted at least once,
-- and how quickly. The "active minter" share (≥80% of theoretical 14-day
-- max) waits on a future join to int_execution_circles_v2_mint_activity_daily;
-- emitted as NULL for now.

SELECT
    toStartOfMonth(invited_at)                                    AS cohort_month,
    count()                                                       AS n_invited,
    countIf(first_mint_at IS NOT NULL)                            AS n_minted_at_least_once,
    round(countIf(first_mint_at IS NOT NULL) / count() * 100, 1)  AS pct_converted,
    quantileExact(0.5)(days_to_first_mint)                        AS median_days_to_first_mint,
    quantileExact(0.25)(days_to_first_mint)                       AS p25_days_to_first_mint,
    quantileExact(0.75)(days_to_first_mint)                       AS p75_days_to_first_mint
FROM {{ ref('int_execution_circles_v2_invite_funnel') }}
WHERE invited_at < today()
GROUP BY cohort_month
ORDER BY cohort_month DESC
