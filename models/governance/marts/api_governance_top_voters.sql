{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_top_voters','granularity:latest']
  )
}}

-- Voter leaderboard: participation + voting power. vp is per-proposal voting
-- power, so total_vp_cast is the sum across all a voter's votes (a cumulative
-- participation-weighted measure, not a point-in-time balance).
SELECT
    voter,
    count()               AS proposals_voted,
    round(sum(vp), 1)     AS total_vp_cast,
    round(avg(vp), 1)     AS avg_vp,
    round(max(vp), 1)     AS max_vp,
    min(created_at)       AS first_vote_at,
    max(created_at)       AS last_vote_at
FROM {{ ref('stg_governance__snapshot_votes') }}
GROUP BY voter
ORDER BY proposals_voted DESC, total_vp_cast DESC
LIMIT 200
