{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_proposal_direction','granularity:latest']
  )
}}

-- Per-proposal voting direction measured TWICE: once by headcount, once by voting power.
-- The gap between the two is the point of this model.
--
-- `head_minus_weight_against` = against-share by voter count minus against-share by voting
-- power. Positive means small holders were MORE opposed than capital; negative means
-- capital was more opposed than the crowd. It is the sharpest available read on whether a
-- decision had broad consent or merely sufficient weight, and no text-based measure can
-- produce it.
--
-- Scope. Only `choice_kind = 'single'` (basic + single-choice, 96.8% of all proposals) is
-- directional. Ranked-choice is excluded from the share columns because Snapshot resolves
-- it by instant-runoff, so a first-preference tally is not a For/Against split -- such
-- proposals still appear, with NULL shares and `directional = 0`.
--
-- Denominators. Shares are over DECISIVE votes (for + against) and deliberately exclude
-- abstain, so an abstain-heavy ballot does not read as consensus. Abstain is exposed
-- separately as its own share of all votes. Every denominator is nullIf-guarded, so a
-- proposal with no decisive votes yields NULL, never a misleading 0.
--
-- Reading the numbers. `voters` is small for many proposals (median 68 per GIP in 2026),
-- so a divergence computed over a handful of voters is noise. Filter on `voters` before
-- ranking -- 30 is a reasonable floor. Non-GIP proposals include spam/phishing ballots
-- (119 of 253); filter `is_gip = 1` for any aggregate.

SELECT sub.*, (SELECT toDate(max(created_at)) FROM {{ ref('int_governance_proposals') }}) AS as_of_date
FROM (
WITH agg AS (
    SELECT
        proposal_id,
        uniqExact(voter)                                              AS voters,
        sum(vp_effective)                                             AS total_vp,
        countIf(polarity = 'for')                                     AS for_voters,
        countIf(polarity = 'against')                                 AS against_voters,
        countIf(polarity = 'abstain')                                 AS abstain_voters,
        countIf(polarity = 'other')                                   AS other_voters,
        sumIf(vp_effective, polarity = 'for')                         AS for_vp,
        sumIf(vp_effective, polarity = 'against')                     AS against_vp,
        sumIf(vp_effective, polarity = 'abstain')                     AS abstain_vp,
        sumIf(vp_effective, polarity = 'other')                       AS other_vp,
        countIf(polarity IN ('for', 'against'))                       AS decisive_voters,
        sumIf(vp_effective, polarity IN ('for', 'against'))           AS decisive_vp
    FROM {{ ref('int_governance_vote_choices') }}
    WHERE choice_kind = 'single'
      AND choice_index_valid
    GROUP BY proposal_id
)

SELECT
    p.id                                        AS proposal_id,
    p.gip_number,
    p.is_gip,
    p.title,
    p.category,
    p.type                                      AS ballot_type,
    p.outcome,
    p.winning_choice,
    p.created_at,
    p.start_at,
    p.end_at,
    p.quorum_met,
    -- 0 for ranked-choice and for proposals with no decodable single-choice votes.
    (a.decisive_voters > 0)                     AS directional,

    a.voters,
    -- Aliased because total_vp also exists on int_governance_proposals; unaliased,
    -- ClickHouse emits the qualified name "a.total_vp" as the column name.
    a.total_vp                                  AS total_vp,
    a.for_voters,
    a.against_voters,
    a.abstain_voters,
    a.other_voters,
    a.for_vp,
    a.against_vp,
    a.abstain_vp,
    a.other_vp,
    a.decisive_voters,
    a.decisive_vp,

    -- The two readings of the same ballot.
    round(a.against_voters / nullIf(a.decisive_voters, 0), 4) AS against_share_by_head,
    round(a.against_vp     / nullIf(a.decisive_vp,     0), 4) AS against_share_by_vp,

    -- THE metric. Positive: the crowd was more opposed than the capital.
    -- Negative: the capital was more opposed than the crowd.
    round(
        a.against_voters / nullIf(a.decisive_voters, 0)
      - a.against_vp     / nullIf(a.decisive_vp,     0)
    , 4)                                        AS head_minus_weight_against,

    -- Abstention is its own signal, measured against ALL votes rather than decisive ones.
    round(a.abstain_voters / nullIf(a.voters, 0), 4)   AS abstain_share_by_head,
    round(a.abstain_vp     / nullIf(a.total_vp, 0), 4) AS abstain_share_by_vp,

    -- How close the decisive vote was. 0 = dead heat, 1 = unanimous.
    round(abs(a.for_vp - a.against_vp)         / nullIf(a.decisive_vp, 0), 4)     AS margin_by_vp,
    round(abs(a.for_voters - a.against_voters) / nullIf(a.decisive_voters, 0), 4) AS margin_by_head

FROM {{ ref('int_governance_proposals') }} AS p
LEFT JOIN agg AS a ON a.proposal_id = p.id
) AS sub
