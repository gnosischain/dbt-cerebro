{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_funnel','granularity:latest']
  )
}}

-- GIP conversion funnel — a true monotonic funnel: every GIP discussed on the
-- forum, how many reached a Snapshot vote, how many passed.
-- (phase-1/2/3 tags are an under-applied, non-monotonic process signal, so they
-- are NOT used as funnel stages here; the has_phase* flags live in
-- int_governance_gip for a separate phase breakdown.)
-- Union is wrapped so the post-UNION ORDER BY can resolve stage_order.
SELECT stage_order, stage, gip_count
FROM (
    SELECT 1 AS stage_order, 'Discussed on forum'   AS stage, toUInt64(countIf(discussed_on_forum))  AS gip_count FROM {{ ref('int_governance_gip') }}
    UNION ALL
    SELECT 2, 'Reached Snapshot vote', toUInt64(countIf(reached_vote))       FROM {{ ref('int_governance_gip') }}
    UNION ALL
    SELECT 3, 'Passed',                toUInt64(countIf(outcome = 'passed')) FROM {{ ref('int_governance_gip') }}
)
ORDER BY stage_order
