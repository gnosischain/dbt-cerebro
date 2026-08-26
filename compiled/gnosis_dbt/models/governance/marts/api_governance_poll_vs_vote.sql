

-- The forum's headcount verdict against the token verdict, on the SAME question.
--
-- This is the only comparison in the layer between two genuinely SEPARATE populations
-- answering one question: a forum temperature check (one person, one vote, no token
-- required) and the Snapshot ballot that followed it (token-weighted). Everything else --
-- including head_minus_weight_against on api_governance_proposal_direction -- compares
-- headcount to capital WITHIN a single ballot, i.e. the same voters weighted two ways.
-- Here the voters are not the same people.
--
-- It resolves frequently, and 8 of 123 pre-vote checks reverse outright. The reversals are
-- NOT symmetric -- 7 of the 8 run the same way, and that asymmetry is the finding:
--
--   community in favour, token vote killed it (7 cases)
--     GIP-119  forum 13/2  (13% against) -> 98.7% against by VP   outcome below_quorum
--     GIP-74   forum 15/3  (17% against) -> 78.1%                 rejected
--     GIP-99   forum 14/1  ( 7% against) -> 55.7%                 rejected
--     GIP-111  forum 11/5  (31% against) -> 62.6%                 below_quorum
--     plus GIP-80, GIP-66, GIP-73 on thin poll turnout
--   community against, token vote passed it (1 case)
--     GIP-120  forum 18/25 (58% against) ->  0.05% against by VP  passed
--
-- So the dominant pattern is community enthusiasm not surviving the token vote, not
-- whales overruling in both directions. Note the failure modes differ: `rejected` is
-- capital actively voting it down, `below_quorum` is capital not showing up at all --
-- read outcome alongside the gap or the two get conflated.
--
-- FILTER decisive_votes BEFORE READING ANY OF THIS. GIP-80's poll is 1 for / 0 against,
-- which is noise, and GIP-66 (7) and GIP-73 (6) are thin. At decisive_votes >= 10 the
-- list is 5 cases and still 4:1 in the same direction.
--
-- A single blended "contestation" score would hide all of this -- both the direction of
-- each reversal and the rejected-versus-below_quorum distinction -- which is why no such
-- score exists here.
--
-- WHAT COUNTS AS A TEMPERATURE CHECK. Only rows where is_temperature_check (the poll
-- offers both a favour and an against option) AND poll_phase = 'pre_discussion' -- i.e.
-- the poll was posted before the proposal existed. Measured: 130 of 142 (poll, proposal)
-- pairs qualify; 2 polls ran during voting and 10 AFTER the ballot closed. A post-close
-- poll is a retrospective, not a temperature check, and reading it as one inverts the
-- causal story. Both are kept as rows with their phase exposed rather than dropped, so the
-- exclusion is visible; filter on is_pre_vote_check for the honest subset.
--
-- GRAIN is (poll, proposal) and is MANY-TO-MANY on both sides. A topic can host several
-- named polls, and links to several proposals when a GIP is re-balloted -- so GIP-120
-- legitimately appears more than once with different poll tallies. poll_name is exposed
-- for exactly this reason. Never aggregate this model without deciding which poll you mean.
--
-- SMALL n. Poll totals run roughly 10-65 votes. These are directional signals, not precise
-- ones; filter on decisive_votes before ranking. Snapshot voter counts are also exposed so
-- the two populations' sizes can be compared rather than assumed.

SELECT
    -- Explicitly aliased: proposal_id exists in both joined relations, and without an alias
    -- ClickHouse emits the QUALIFIED name ("d.proposal_id") as the column name, which
    -- breaks every consumer and every test referring to it.
    d.proposal_id                                   AS proposal_id,
    d.gip_number,
    d.is_gip,
    d.title,
    d.category,
    d.outcome,
    d.created_at                                    AS proposal_created_at,
    d.start_at,
    d.end_at,

    p.topic_id                                      AS topic_id,
    p.post_id                                       AS poll_post_id,
    p.poll_name,
    p.status                                        AS poll_status,
    p.poll_posted_at,
    p.voters                                        AS poll_voters,
    p.favour_votes,
    p.against_votes,
    p.abstain_votes,
    p.decisive_votes,
    p.has_withheld_counts,
    p.is_temperature_check,

    -- Where the poll sits relative to this proposal's lifecycle. 'pre_discussion' is the
    -- only phase that makes the poll a genuine temperature check.
    
multiIf(
    p.poll_posted_at <  d.created_at, 'pre_discussion',
    p.poll_posted_at <  d.start_at,   'pre_vote',
    p.poll_posted_at <  d.end_at,     'voting',
    'post_close'
) AS poll_phase,
    (
        p.is_temperature_check
        AND 
multiIf(
    p.poll_posted_at <  d.created_at, 'pre_discussion',
    p.poll_posted_at <  d.start_at,   'pre_vote',
    p.poll_posted_at <  d.end_at,     'voting',
    'post_close'
) = 'pre_discussion'
    )                                               AS is_pre_vote_check,

    -- The three readings of the same question.
    p.poll_against_share,
    d.against_share_by_head                         AS vote_against_share_by_head,
    d.against_share_by_vp                           AS vote_against_share_by_vp,
    d.voters                                        AS vote_voters,

    -- Forum headcount minus token weight. POSITIVE means the community was more opposed
    -- than the capital that ultimately decided it; NEGATIVE means capital was more opposed
    -- than the community. NULL when the poll's counts are withheld or nothing decisive was
    -- cast on either side.
    round(p.poll_against_share - d.against_share_by_vp, 4)   AS poll_minus_vote_vp,
    -- Against the ballot's own headcount instead: isolates "different people" from
    -- "different weighting", since both sides of this one are one-person-one-vote.
    round(p.poll_against_share - d.against_share_by_head, 4) AS poll_minus_vote_head,
    -- True where the two populations landed on opposite sides of 50%. The headline cases.
    (
        p.poll_against_share IS NOT NULL
        AND d.against_share_by_vp IS NOT NULL
        AND ((p.poll_against_share > 0.5) != (d.against_share_by_vp > 0.5))
    )                                               AS verdicts_disagree,

    -- Freshness anchor for the whole view, NOT a per-row date -- every row carries the
    -- same value. Taken from the poll side, since that is what this endpoint adds over
    -- api_governance_proposal_direction: the latest poll observation it can see. Polls are
    -- ReplacingMergeTree with block-level insert dedup, so an unchanged poll writes nothing
    -- and this can legitimately sit still while the forum moves.
    (SELECT toDate(max(poll_posted_at)) FROM `dbt`.`int_governance_poll_sentiment`) AS as_of_date

FROM `dbt`.`int_governance_poll_sentiment` AS p
INNER JOIN `dbt`.`int_governance_proposal_topic_links` AS l
    ON l.topic_id = p.topic_id
INNER JOIN `dbt`.`api_governance_proposal_direction` AS d
    ON d.proposal_id = l.proposal_id
WHERE d.directional