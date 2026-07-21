{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_kpis','granularity:latest']
  )
}}

-- Single-row headline KPIs for governance number tiles.
SELECT
    (SELECT count() FROM {{ ref('int_governance_proposals') }})                                   AS total_proposals,
    (SELECT countIf(is_gip) FROM {{ ref('int_governance_proposals') }})                           AS total_gip_proposals,
    (SELECT count() FROM {{ ref('int_governance_gip') }})                                         AS total_gips,
    (SELECT uniqExact(voter) FROM {{ ref('stg_governance__snapshot_votes') }})                    AS unique_voters,
    (SELECT count() FROM {{ ref('stg_governance__snapshot_votes') }})                             AS total_votes_cast,
    (SELECT max(followers_count) FROM {{ ref('stg_governance__snapshot_space') }})                AS followers,
    (SELECT countIf(outcome = 'passed') FROM {{ ref('int_governance_proposals') }})               AS proposals_passed,
    (SELECT round(countIf(outcome = 'passed')
                  / nullIf(countIf(outcome IN ('passed','rejected','below_quorum')), 0), 3)
     FROM {{ ref('int_governance_proposals') }})                                                  AS pass_rate,
    (SELECT round(avg(unique_voters), 1) FROM {{ ref('int_governance_proposals') }} WHERE state = 'closed') AS avg_voters_per_closed_proposal
