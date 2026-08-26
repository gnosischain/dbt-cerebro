-- Invariant: summing vp_effective per proposal in int_governance_vote_choices
-- reconstructs Snapshot's own scores_total for every closed proposal with final
-- scores (ranked ballots contribute vp once via preference_rank 1). A failing
-- row means voting power was dropped or double-counted by the choice explosion.
SELECT
    p.id AS proposal_id,
    p.scores_total,
    r.vp_sum,
    abs(r.vp_sum - p.scores_total) AS abs_diff
FROM (
    SELECT proposal_id, sum(vp_effective) AS vp_sum
    FROM {{ ref('int_governance_vote_choices') }}
    GROUP BY proposal_id
) AS r
INNER JOIN {{ ref('int_governance_proposals') }} AS p
    ON p.id = r.proposal_id
WHERE p.scores_state = 'final'
  AND p.scores_total > 0
  AND abs(r.vp_sum - p.scores_total) > greatest(0.5, p.scores_total * 0.001)
