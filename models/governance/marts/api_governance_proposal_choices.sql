{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_proposal_choices','granularity:latest']
  )
}}

-- Per-choice vote breakdown, long format: one row per (proposal, choice).
-- choices[] and scores[] are positionally aligned in Snapshot's API, so a
-- parallel ARRAY JOIN (not a cross join) zips them correctly -- verified
-- against real data: 253 proposals with choices -> 253 distinct winner rows,
-- i.e. exactly one is_winner=1 per proposal. Powers the per-proposal
-- For/Against/Abstain (or ranked-choice) bar with a quorum reference line.
SELECT
    id AS proposal_id,
    gip_number,
    title,
    outcome,
    winning_choice,
    quorum,
    scores_total,
    choice,
    score,
    round(score / nullIf(scores_total, 0), 4) AS pct,
    choice = winning_choice AS is_winner
FROM {{ ref('int_governance_proposals') }}
ARRAY JOIN choices AS choice, scores AS score
WHERE length(choices) > 0
