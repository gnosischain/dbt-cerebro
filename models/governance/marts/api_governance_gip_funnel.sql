{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_funnel','granularity:latest']
  )
}}

-- Nested GIP conversion funnel (each stage is a subset of the previous):
--   1) discussed on forum
--   2) discussed AND reached a Snapshot vote
--   3) discussed AND reached vote AND enacted (has_passed OR has_decided)
-- Uses spine flags so multi-proposal GIPs count as enacted when any ballot
-- passed/decided, without requiring a non-null canonical outcome (ambiguous
-- number-reuse cases can still be enacted via has_passed).
-- phase-1/2/3 tags are NOT funnel stages (see has_phase* on int_governance_gip).
SELECT stage_order, stage, gip_count
FROM (
    SELECT
        1 AS stage_order,
        'Discussed on forum' AS stage,
        toUInt64(countIf(discussed_on_forum)) AS gip_count
    FROM {{ ref('int_governance_gip') }}

    UNION ALL

    SELECT
        2,
        'Reached Snapshot vote',
        toUInt64(countIf(discussed_on_forum AND reached_vote))
    FROM {{ ref('int_governance_gip') }}

    UNION ALL

    SELECT
        3,
        'Passed or enacted',
        toUInt64(countIf(discussed_on_forum AND reached_vote AND (has_passed OR has_decided)))
    FROM {{ ref('int_governance_gip') }}
)
ORDER BY stage_order
