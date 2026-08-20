{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_discussion_phases','granularity:latest']
  )
}}

-- Discussion and endorsement per proposal per lifecycle phase: how much was said before
-- the proposal existed, after it existed but before voting opened, during the vote, and
-- after it closed.
--
-- Two like measures, and the difference between them matters:
--   likes_given_in_phase        - counted by the LIKE's own timestamp. Exact. This is
--                                 the real "endorsement during the vote" number.
--   likes_on_posts_written_in_phase - sum of the current like_count of posts WRITTEN in
--                                 the phase. Approximate, and a like given today on a
--                                 2023 post lands in 2023. Kept only so the exact figure
--                                 can be reconciled against forum_posts, which is the
--                                 only number the mini-app can currently show.
-- Prefer the first. If they diverge sharply, the second is the one that is wrong.
--
-- Phase grid. Rows exist for any (proposal, phase) with either posts or likes -- built
-- from the union of both, so a phase where people only clicked like and nobody wrote is
-- still present. Absent phases mean genuinely nothing happened; a zero means the phase
-- had activity of the other kind. Never read an absent row as a zero.
--
-- Coverage. Only proposals reachable through int_governance_proposal_topic_links appear:
-- 137 of 253 (54%), effectively all GIP proposals. Filter is_gip = 1 for aggregates --
-- 119 of the 253 are non-GIP spam/phishing ballots.

WITH post_side AS (
    SELECT
        proposal_id,
        phase,
        count()                                     AS posts,
        uniqExact(user_id)                          AS participants,
        countIf(has_question)                       AS question_posts,
        countIf(is_short_procedural)                AS short_procedural_posts,
        countIf(reply_to_post_number > 0)           AS reply_posts,
        sum(post_like_count_current)                AS likes_on_posts_written_in_phase,
        min(posted_at)                              AS first_post_at,
        max(posted_at)                              AS last_post_at
    FROM {{ ref('int_governance_forum_post_phases') }}
    GROUP BY proposal_id, phase
),

like_side AS (
    SELECT
        l.proposal_id                               AS proposal_id,
        {{ governance_phase_bucket('lk.created_at', 'pr.created_at', 'pr.start_at', 'pr.end_at') }} AS phase,
        count()                                     AS likes_given_in_phase,
        uniqExact(lk.acting_username)               AS distinct_likers
    FROM {{ ref('stg_governance__forum_likes') }} AS lk
    INNER JOIN {{ ref('int_governance_proposal_topic_links') }} AS l
        ON l.topic_id = lk.topic_id
    INNER JOIN {{ ref('int_governance_proposals') }} AS pr
        ON pr.id = l.proposal_id
    GROUP BY l.proposal_id, phase
),

spine AS (
    SELECT proposal_id, phase FROM post_side
    UNION DISTINCT
    SELECT proposal_id, phase FROM like_side
)

SELECT
    -- Explicitly aliased: proposal_id and phase exist in all three CTEs, and without an
    -- alias ClickHouse emits the QUALIFIED name ("s.proposal_id") as the column name,
    -- forcing every consumer to quote it. Unambiguous columns below need no alias.
    s.proposal_id                                   AS proposal_id,
    pr.gip_number,
    pr.is_gip,
    pr.title,
    pr.category,
    pr.outcome,
    pr.created_at                                   AS proposal_created_at,
    pr.start_at,
    pr.end_at,
    s.phase                                         AS phase,
    -- Unmatched LEFT JOIN rows yield 0 in ClickHouse, not NULL. That is the correct value
    -- here (a phase with likes but no posts really did have zero posts), so these are
    -- left as-is rather than coalesced -- a coalesce would be a no-op anyway.
    p.posts,
    p.participants,
    p.question_posts,
    p.short_procedural_posts,
    p.reply_posts,
    p.likes_on_posts_written_in_phase,
    lk.likes_given_in_phase,
    lk.distinct_likers,
    p.first_post_at,
    p.last_post_at,
    -- Share of posts in this phase that ask something rather than assert. A friction
    -- proxy with no sentiment model behind it. NULL when the phase has no posts.
    round(p.question_posts / nullIf(p.posts, 0), 4) AS question_share,
    -- Freshness anchor required of a point-in-time endpoint. Taken across BOTH
    -- activity sources rather than posts alone: a phase can be present on likes
    -- only, so a posts-only anchor would under-report the date whenever likes
    -- arrived more recently. Scoped to post_phases (not raw forum_posts) because
    -- that is the linked-topic subset this endpoint can actually see.
    greatest(
        (SELECT toDate(max(posted_at)) FROM {{ ref('int_governance_forum_post_phases') }}),
        (SELECT toDate(max(created_at)) FROM {{ ref('stg_governance__forum_likes') }})
    )                                               AS as_of_date
FROM spine AS s
LEFT JOIN post_side AS p
    ON p.proposal_id = s.proposal_id AND p.phase = s.phase
LEFT JOIN like_side AS lk
    ON lk.proposal_id = s.proposal_id AND lk.phase = s.phase
INNER JOIN {{ ref('int_governance_proposals') }} AS pr
    ON pr.id = s.proposal_id
