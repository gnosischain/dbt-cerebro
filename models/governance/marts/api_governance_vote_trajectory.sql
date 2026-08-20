{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_vote_trajectory','granularity:hourly']
  )
}}

-- How opinion FORMED across a proposal's voting window, not just how it ended.
-- One row per (proposal, hour since voting opened) carrying the cumulative against-share
-- by headcount and by voting power.
--
-- Why an hourly curve is viable here (measured 2026-07-28 across 210 closed proposals):
-- the median voting window is 168h, the median proposal collects 100 votes spread over 50
-- DISTINCT active hours, and while 36% of votes land in the first 24h only 4.8% land in
-- the last 24h. Voting is front-loaded but far from a single spike, so the curve has real
-- shape rather than being a step function. 142 proposals carry 50+ votes and 70 carry 200+.
--
-- What it is for. Overlay forum post timestamps on this curve: where the against-share
-- inflects sharply near a post, that post is a candidate for having moved opinion. Use
-- `delta_against_share_by_vp` to locate those hours. This is a COINCIDENCE DETECTOR, not a
-- causal claim -- label any UI built on it "posts near an inflection", never "posts that
-- caused". A proposal can inflect because one whale voted, which is why the headcount and
-- voting-power curves are both exposed: a spike in one but not the other tells you which.
--
-- Caveats. Restricted to `choice_kind = 'single'`; ranked-choice has no meaningful
-- running For/Against split. Hours with no votes are ABSENT rather than carried forward,
-- so a consumer plotting a line must either step-interpolate or accept gaps -- do not
-- read a gap as a flat period of zero support. `hour_offset` is floored at 0; a vote
-- timestamped fractionally before `start_at` (clock skew) lands in hour 0 rather than
-- being dropped. Filter on `proposal_total_votes` before drawing -- a curve over 8 votes
-- is noise.

WITH v AS (
    SELECT
        proposal_id,
        gip_number,
        is_gip,
        greatest(0, dateDiff('hour', start_at, created_at)) AS hour_offset,
        vp_effective,
        polarity
    FROM {{ ref('int_governance_vote_choices') }}
    WHERE choice_kind = 'single'
      AND choice_index_valid
),

bucketed AS (
    SELECT
        proposal_id,
        any(gip_number)                                     AS gip_number,
        any(is_gip)                                         AS is_gip,
        hour_offset,
        count()                                             AS votes,
        sum(vp_effective)                                   AS vp,
        countIf(polarity = 'against')                       AS against_votes,
        sumIf(vp_effective, polarity = 'against')           AS against_vp,
        countIf(polarity IN ('for', 'against'))             AS decisive_votes,
        sumIf(vp_effective, polarity IN ('for', 'against')) AS decisive_vp
    FROM v
    GROUP BY proposal_id, hour_offset
),

cumulative AS (
    SELECT
        proposal_id,
        gip_number,
        is_gip,
        hour_offset,
        votes,
        vp,
        sum(votes)          OVER w AS cum_votes,
        sum(vp)             OVER w AS cum_vp,
        sum(against_votes)  OVER w AS cum_against_votes,
        sum(against_vp)     OVER w AS cum_against_vp,
        sum(decisive_votes) OVER w AS cum_decisive_votes,
        sum(decisive_vp)    OVER w AS cum_decisive_vp
    FROM bucketed
    WINDOW w AS (
        PARTITION BY proposal_id
        ORDER BY hour_offset
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

shares AS (
    SELECT
        proposal_id,
        gip_number,
        is_gip,
        hour_offset,
        votes                                                          AS votes_in_hour,
        round(vp, 2)                                                   AS vp_in_hour,
        cum_votes,
        round(cum_vp, 2)                                               AS cum_vp,
        round(cum_against_votes / nullIf(cum_decisive_votes, 0), 4)    AS cum_against_share_by_head,
        round(cum_against_vp    / nullIf(cum_decisive_vp,    0), 4)    AS cum_against_share_by_vp,
        max(cum_votes) OVER (PARTITION BY proposal_id)                 AS proposal_total_votes
    FROM cumulative
)

SELECT
    proposal_id,
    gip_number,
    is_gip,
    hour_offset,
    votes_in_hour,
    vp_in_hour,
    cum_votes,
    cum_vp,
    proposal_total_votes,
    -- Share of the proposal's eventual turnout that had arrived by this hour.
    round(cum_votes / nullIf(proposal_total_votes, 0), 4) AS turnout_progress,
    cum_against_share_by_head,
    cum_against_share_by_vp,
    -- Hour-over-hour movement. Large absolute values are the inflection candidates to
    -- align against forum activity. NULL in a proposal's first bucket (nothing to compare).
    round(
        cum_against_share_by_vp
      - lagInFrame(cum_against_share_by_vp) OVER (
            PARTITION BY proposal_id ORDER BY hour_offset
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    , 4)                                                   AS delta_against_share_by_vp
FROM shares
