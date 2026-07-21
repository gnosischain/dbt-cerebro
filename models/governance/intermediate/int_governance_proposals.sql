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
-- then LEFT JOINed, so proposals with no votes get 0, never an epoch sentinel.
--
-- Outcome is derived from the WINNING choice, not a hard-coded For/Against:
--   passed        - an affirmative option won (For/Yes/Approve/Enable/Extend/
--                   "Let's do this!"/... or, on a 2-option ballot, any non-
--                   negative winner)
--   rejected      - a negative / status-quo option won (Against/No/"Make no
--                   change"/"Don't ..."/...)
--   no_consensus  - Abstain won
--   decided       - a specific option won on a multi-option SELECTION ballot
--                   (e.g. GIP-148 -> "Noctua (Noca)"); the real result is in
--                   winning_choice, so it is neither pass nor fail
--   below_quorum  - closed & final but quorum not met (or zero votes)
--   open          - not closed, or scores not yet final
-- winning_choice always carries the actual winning option regardless of bucket.
WITH votes_agg AS (
    SELECT
        proposal_id,
        count()          AS unique_voters,
        sum(vp)          AS total_vp,
        min(created_at)  AS first_vote_at,
        max(created_at)  AS last_vote_at
    FROM {{ ref('stg_governance__snapshot_votes') }}
    GROUP BY proposal_id
),
base AS (
    SELECT
        *,
        if(length(scores) > 0, choices[indexOf(scores, arrayMax(scores))], '') AS winning_choice,
        (quorum = 0 OR scores_total >= quorum)                                  AS quorum_met
    FROM {{ ref('stg_governance__snapshot_proposals') }}
)
SELECT
    b.id,
    b.space_id,
    b.gip_number,
    b.gip_number IS NOT NULL AS is_gip,
    b.title,
    b.state,
    b.type,
    b.author,
    b.created_at,
    b.start_at,
    b.end_at,
    b.quorum,
    b.scores_total,
    b.scores_state,
    b.votes_count,
    b.choices,
    b.scores,
    b.strategy_names,
    b.strategy_networks,
    b.winning_choice,
    b.quorum_met,
    multiIf(
        b.state != 'closed',                                'open',
        b.scores_state != 'final',                          'open',
        length(b.scores) = 0 OR b.scores_total = 0,         'below_quorum',
        NOT b.quorum_met,                                   'below_quorum',
        match(lower(b.winning_choice), '^abstain'),         'no_consensus',
        match(lower(b.winning_choice), '(\\bagainst\\b|\\bno\\b|\\bnay\\b|reject|make no change|do not|\\bdon.?t\\b|do nothing|status quo|not now|\\bnone\\b)'), 'rejected',
        match(lower(b.winning_choice), '(\\bfor\\b|\\byes\\b|approve|adopt|enact|accept|in favou?r|\\baye\\b|agree|support|let.?s do|proceed|enable|extend|launch|activate|ratify)'), 'passed',
        length(b.choices) <= 2,                             'passed',
        'decided'
    ) AS outcome,
    multiIf(
        match(lower(b.title), '(treasury|fund|grant|budget|spend|allocat|compensat|payment|reimburs|runway|financ)'), 'Treasury & Funding',
        match(lower(b.title), '(safe|token|chain|protocol|technical|upgrade|contract|bridge|validator|staking|fork|deploy|parameter|merge|mainnet|client)'), 'Technical & Protocol',
        match(lower(b.title), '(partner|integrat|listing|collaborat|acquisition|launch)'), 'Partnerships & Ecosystem',
        match(lower(b.title), '(governance|meta|process|constitution|delegate|framework|charter|structure|quorum)'), 'Meta & Governance',
        'Other'
    ) AS category,
    coalesce(va.unique_voters, 0)  AS unique_voters,
    coalesce(va.total_vp, 0.0)     AS total_vp,
    va.first_vote_at,
    va.last_vote_at
FROM base AS b
LEFT JOIN votes_agg AS va ON b.id = va.proposal_id
