{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_concentration','granularity:latest']
  )
}}

-- Space-level concentration at the canonical tiers 10/20/50, long format:
-- one row per (population, tier). Populations: voting power cast per voter
-- (all-time, participation-weighted -- not a balance), ballots cast per
-- voter, and delegators per delegate (currently active delegations, both
-- registries). Per-proposal whale concentration (tiers 1/5/10) is a
-- different grain and lives in api_governance_whale_concentration.

WITH voters_vp AS (
    SELECT groupArray(v) AS vals, sum(v) AS total
    FROM (
        SELECT sum(vp) AS v
        FROM {{ ref('stg_governance__snapshot_votes') }}
        GROUP BY voter
        ORDER BY v DESC
    )
),

voters_votes AS (
    SELECT groupArray(v) AS vals, sum(v) AS total
    FROM (
        SELECT toFloat64(count()) AS v
        FROM {{ ref('stg_governance__snapshot_votes') }}
        GROUP BY voter
        ORDER BY v DESC
    )
),

delegates AS (
    SELECT groupArray(v) AS vals, sum(v) AS total
    FROM (
        SELECT toFloat64(uniqExact(delegator)) AS v
        FROM {{ ref('int_governance_current_delegations') }}
        GROUP BY delegate
        ORDER BY v DESC
    )
)

SELECT
    sub.population AS population,
    sub.tier AS tier,
    sub.tier_value AS tier_value,
    sub.total_value AS total_value,
    sub.share AS share,
    sub.members AS members,
    (SELECT toDate(max(created_at)) FROM {{ ref('stg_governance__snapshot_votes') }}) AS as_of_date
FROM (
    SELECT
        'voters_by_vp' AS population,
        tier,
        arraySum(arraySlice(vals, 1, tier)) AS tier_value,
        total AS total_value,
        arraySum(arraySlice(vals, 1, tier)) / nullIf(total, 0) AS share,
        length(vals) AS members
    FROM voters_vp
    ARRAY JOIN [10, 20, 50] AS tier

    UNION ALL

    SELECT
        'voters_by_votes' AS population,
        tier,
        arraySum(arraySlice(vals, 1, tier)),
        total,
        arraySum(arraySlice(vals, 1, tier)) / nullIf(total, 0),
        length(vals)
    FROM voters_votes
    ARRAY JOIN [10, 20, 50] AS tier

    UNION ALL

    SELECT
        'delegates_by_delegators' AS population,
        tier,
        arraySum(arraySlice(vals, 1, tier)),
        total,
        arraySum(arraySlice(vals, 1, tier)) / nullIf(total, 0),
        length(vals)
    FROM delegates
    ARRAY JOIN [10, 20, 50] AS tier
) AS sub
ORDER BY population, tier
