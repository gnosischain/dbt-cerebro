{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_proposals','granularity:latest']
  )
}}

-- Proposal browse table for the dashboard. Filter is_gip = 1 to exclude
-- non-GIP announcement proposals.
SELECT
    id,
    gip_number,
    is_gip,
    title,
    category,
    author,
    created_at,
    start_at,
    end_at,
    state,
    scores_state,
    outcome,
    winning_choice,
    quorum,
    quorum_met,
    scores_total,
    votes_count,
    unique_voters,
    total_vp
FROM {{ ref('int_governance_proposals') }}
ORDER BY created_at DESC
