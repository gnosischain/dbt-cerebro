{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(created_at, id)',
    tags=['production','governance','proposals']
  )
}}

-- One enriched row per Snapshot proposal: outcome, quorum, category, and vote
-- aggregates. Vote counts are pre-aggregated in a CTE (one row per proposal)
-- then LEFT JOINed, so proposals with no votes get NULL (-> 0), never an epoch
-- sentinel from a min() over an unmatched non-aggregated join.
WITH votes_agg AS (
    SELECT
        proposal_id,
        count()          AS unique_voters,
        sum(vp)          AS total_vp,
        min(created_at)  AS first_vote_at,
        max(created_at)  AS last_vote_at
    FROM {{ ref('stg_governance__snapshot_votes') }}
    GROUP BY proposal_id
)
SELECT
    p.id,
    p.space_id,
    p.gip_number,
    p.gip_number IS NOT NULL                              AS is_gip,
    p.title,
    p.state,
    p.type,
    p.author,
    p.created_at,
    p.start_at,
    p.end_at,
    p.quorum,
    p.scores_total,
    p.scores_state,
    p.votes_count,
    p.choices,
    p.scores,
    p.strategy_names,
    p.strategy_networks,
    -- Winning choice = label at the max-score index (1-based; ties -> first).
    if(length(p.scores) > 0, p.choices[indexOf(p.scores, arrayMax(p.scores))], '') AS winning_choice,
    (p.quorum = 0 OR p.scores_total >= p.quorum)          AS quorum_met,
    multiIf(
        p.state != 'closed',                              'open',
        p.scores_state != 'final',                        'open',
        NOT (p.quorum = 0 OR p.scores_total >= p.quorum), 'below_quorum',
        match(lower(if(length(p.scores) > 0, p.choices[indexOf(p.scores, arrayMax(p.scores))], '')),
              '^(for|yes|approve|adopt|accept|enact|in favor|in favour|yea|aye)'), 'passed',
        'rejected'
    )                                                     AS outcome,
    multiIf(
        match(lower(p.title), '(treasury|fund|grant|budget|spend|allocat|compensat|payment|reimburs|runway|financ)'), 'Treasury & Funding',
        match(lower(p.title), '(safe|token|chain|protocol|technical|upgrade|contract|bridge|validator|staking|fork|deploy|parameter|merge|mainnet|client)'), 'Technical & Protocol',
        match(lower(p.title), '(partner|integrat|listing|collaborat|acquisition|launch)'), 'Partnerships & Ecosystem',
        match(lower(p.title), '(governance|meta|process|constitution|delegate|framework|charter|structure|quorum)'), 'Meta & Governance',
        'Other'
    )                                                     AS category,
    coalesce(va.unique_voters, 0)                         AS unique_voters,
    coalesce(va.total_vp, 0.0)                            AS total_vp,
    va.first_vote_at,
    va.last_vote_at
FROM {{ ref('stg_governance__snapshot_proposals') }} p
LEFT JOIN votes_agg va ON p.id = va.proposal_id
