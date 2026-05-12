{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_invite_funnel_cohort','granularity:monthly']
  )
}}

-- Cohort-by-invitation-month funnel. Stages skip the trivial acceptance mint
-- (which always fires) and surface the real cadence drop-off:
--
--   Invited  →  ≥2 mint-days in first 30d  →  ≥7  →  ≥14  →  Active Minter (ever)
--
-- Latency stats (median / p25 / p75 days to second mint) describe how fast
-- the invitees who came back actually came back; NULLs are excluded from
-- the percentile computation.

WITH base AS (
    SELECT
        toStartOfMonth(invited_at) AS cohort_month,
        n_mint_days_first_30d,
        became_active_minter_at,
        days_to_second_mint
    FROM {{ ref('int_execution_circles_v2_invite_funnel') }}
    WHERE invited_at < today()
)

SELECT
    cohort_month                                                                   AS cohort_month,
    count()                                                                        AS n_invited,
    countIf(n_mint_days_first_30d >= 2)                                            AS n_minted_2_days,
    countIf(n_mint_days_first_30d >= 7)                                            AS n_minted_7_days,
    countIf(n_mint_days_first_30d >= 14)                                           AS n_minted_14_days,
    countIf(became_active_minter_at IS NOT NULL)                                   AS n_active_minter,
    round(countIf(n_mint_days_first_30d >= 2)  / count() * 100, 1)                 AS pct_minted_2_days,
    round(countIf(n_mint_days_first_30d >= 7)  / count() * 100, 1)                 AS pct_minted_7_days,
    round(countIf(n_mint_days_first_30d >= 14) / count() * 100, 1)                 AS pct_minted_14_days,
    round(countIf(became_active_minter_at IS NOT NULL) / count() * 100, 1)         AS pct_active_minter,
    quantileExactIf(0.5)(days_to_second_mint,  days_to_second_mint IS NOT NULL)    AS median_days_to_second_mint,
    quantileExactIf(0.25)(days_to_second_mint, days_to_second_mint IS NOT NULL)    AS p25_days_to_second_mint,
    quantileExactIf(0.75)(days_to_second_mint, days_to_second_mint IS NOT NULL)    AS p75_days_to_second_mint
FROM base
GROUP BY cohort_month
ORDER BY cohort_month DESC
