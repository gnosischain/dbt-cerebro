{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_apy"]
    )
}}

-- Network-wide daily APY distribution, built fresh on int_consensus_validators_income_daily
-- (which is now spec-bounded — see the v2 header comment on that model). Parallel to the
-- frozen int_consensus_validators_dists_daily lineage which reads per_index_apy_daily and
-- is now restricted to that legacy consumer chain only.
--
-- Filter rationale:
--   * apy BETWEEN 0 AND 200 drops exited validators (apy=0) and numerical-noise outliers.
--   * effective_balance_gno > 0 drops exited / pending-queued validators without requiring
--     a status column on the source (income_daily is per-validator but not status-joined).
--
-- Output columns mirror int_consensus_validators_dists_daily for the APY half; no balance
-- quantiles here (use int_consensus_validators_dists_daily for balances — that's frozen and
-- still correct since per_index_apy is balance-driven).

SELECT
    date,
    q_apy[1]  AS q05_apy,
    q_apy[2]  AS q10_apy,
    q_apy[3]  AS q25_apy,
    q_apy[4]  AS q50_apy,
    q_apy[5]  AS q75_apy,
    q_apy[6]  AS q90_apy,
    q_apy[7]  AS q95_apy,
    avg_apy_weighted,
    validators_included
FROM (
    SELECT
        toStartOfDay(date) AS date
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(apy) AS q_apy
        -- Balance-weighted mean, matching fct_consensus_validators_apy_mean_daily so the
        -- overlay line on the dashboard band chart reads consistently with the legacy KPI.
        ,SUMIf(apy * balance_prev_gno, apy > 0 AND apy < 200 AND balance_prev_gno > 0)
         / NULLIF(SUMIf(balance_prev_gno, apy > 0 AND apy < 200 AND balance_prev_gno > 0), 0)
            AS avg_apy_weighted
        ,count() AS validators_included
    FROM {{ ref('int_consensus_validators_income_daily') }}
    WHERE apy > 0 AND apy < 200                     -- outlier filter
      AND effective_balance_gno > 0                 -- drop exited / pending
      {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
    GROUP BY 1
)
