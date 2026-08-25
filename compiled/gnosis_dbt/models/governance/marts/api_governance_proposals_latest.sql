

-- Proposal browse table for the dashboard. Filter is_gip = 1 to exclude
-- non-GIP announcement proposals.
SELECT sub.*, (SELECT toDate(max(created_at)) FROM `dbt`.`int_governance_proposals`) AS as_of_date
FROM (
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
FROM `dbt`.`int_governance_proposals`
ORDER BY created_at DESC
) AS sub