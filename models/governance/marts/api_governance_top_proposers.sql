{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_top_proposers','granularity:latest']
  )
}}

-- Proposer leaderboard: activity + outcome record.
-- enactment_rate = share of closed proposals that resolved productively
-- (passed OR decided). Distinct from gip_pass_rate on KPIs, which is
-- pass/fail only and excludes selection ballots (decided).
SELECT
    author,
    count()                                    AS proposals_authored,
    countIf(is_gip)                            AS gips_authored,
    countIf(outcome IN ('passed', 'decided'))   AS enacted,
    countIf(outcome = 'rejected')               AS rejected,
    countIf(outcome = 'no_consensus')           AS no_consensus,
    countIf(outcome = 'below_quorum')           AS below_quorum,
    round(
        countIf(outcome IN ('passed', 'decided'))
        / nullIf(countIf(outcome != 'open'), 0), 3
    )                                           AS enactment_rate,
    min(created_at)                             AS first_proposal_at,
    max(created_at)                             AS last_proposal_at
FROM {{ ref('int_governance_proposals') }}
GROUP BY author
ORDER BY proposals_authored DESC
LIMIT 50
