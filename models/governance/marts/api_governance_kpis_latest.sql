{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_kpis','granularity:latest']
  )
}}

-- Single-row headline governance KPIs. Pass rate + passed count are GIP-scoped
-- so the ~118 non-GIP announcement/spam proposals don't skew the headline
-- (blended pass rate is misleading; real GIP pass rate is ~80%). Selection
-- ballots (outcome='decided') are excluded from the pass-rate denominator since
-- they are not pass/fail.
SELECT
    (SELECT count() FROM {{ ref('int_governance_proposals') }})                                    AS total_proposals,
    (SELECT countIf(is_gip) FROM {{ ref('int_governance_proposals') }})                            AS total_gip_proposals,
    (SELECT count() FROM {{ ref('int_governance_gip') }})                                          AS total_gips,
    (SELECT uniqExact(voter) FROM {{ ref('stg_governance__snapshot_votes') }})                     AS unique_voters,
    (SELECT count() FROM {{ ref('stg_governance__snapshot_votes') }})                              AS total_votes_cast,
    (SELECT max(followers_count) FROM {{ ref('stg_governance__snapshot_space') }})                 AS followers,
    (SELECT countIf(is_gip AND outcome = 'passed') FROM {{ ref('int_governance_proposals') }})     AS gip_proposals_passed,
    (SELECT round(
        countIf(is_gip AND outcome = 'passed')
        / nullIf(countIf(is_gip AND outcome IN ('passed','rejected','no_consensus','below_quorum')), 0), 3)
     FROM {{ ref('int_governance_proposals') }})                                                   AS gip_pass_rate,
    (SELECT round(avg(unique_voters), 1)
     FROM {{ ref('int_governance_proposals') }} WHERE is_gip AND state = 'closed')                 AS avg_voters_per_gip
