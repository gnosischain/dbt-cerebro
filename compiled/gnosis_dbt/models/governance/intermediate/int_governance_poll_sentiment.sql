

-- One row per (post, poll): a forum poll collapsed into a for/against tally, with the
-- timing of the post that hosts it.
--
-- Why this matters. Of the whole governance corpus this is the ONLY place where
-- one-person-one-vote community sentiment is recorded on a question that later receives a
-- token-weighted Snapshot vote. 189 polls exist, 75 of them carrying an
-- "In Favour"/"Against" temperature check. It is the missing middle of the
-- discussion -> signal -> vote funnel.
--
-- TIMING COMES FROM THE HOST POST. The poll row itself has no created_at -- close_at is a
-- schedule and ingested_at is merely first-seen. So the poll is dated by the post that
-- contains it, joined on post_id (coverage verified: 184 of 184 host posts resolve). An
-- INNER JOIN is correct here: a poll whose host post is unknown cannot be dated, and an
-- undated poll must not silently acquire a phase downstream.
--
-- GRAIN is (post, poll), not post. A single post can host several named polls
-- ("poll", "poll2"), so poll_name is part of the key.
--
-- is_temperature_check requires BOTH a favour and an against option. A poll offering only
-- one direction, or a multi-option selection ballot, is not a for/against check and must
-- not be read as one -- its against_share would be meaningless.
--
-- against_share is NULL, not 0, whenever the decisive total is zero or any option's count
-- is withheld. A hidden live poll reading as 0% opposition would be worse than no number.

WITH opts AS (
    SELECT
        post_id,
        topic_id,
        poll_name,
        any(poll_id)                                            AS poll_id,
        any(poll_type)                                          AS poll_type,
        any(status)                                             AS status,
        any(results_visibility)                                 AS results_visibility,
        any(is_public)                                          AS is_public,
        any(close_at)                                           AS close_at,
        any(voters)                                             AS voters,
        count()                                                 AS option_count,
        max(votes_withheld)                                     AS has_withheld_counts,
        sumIf(option_votes, option_polarity = 'for')             AS favour_votes,
        sumIf(option_votes, option_polarity = 'against')         AS against_votes,
        sumIf(option_votes, option_polarity = 'abstain')         AS abstain_votes,
        sumIf(option_votes, option_polarity = 'other')           AS other_votes,
        countIf(option_polarity = 'for')                         AS favour_options,
        countIf(option_polarity = 'against')                     AS against_options
    FROM `dbt`.`stg_governance__forum_polls`
    GROUP BY post_id, topic_id, poll_name
)

SELECT
    o.post_id,
    o.topic_id AS topic_id,
    o.poll_name,
    o.poll_id,
    o.poll_type,
    o.status,
    o.results_visibility,
    o.is_public,
    o.close_at,
    o.voters,
    o.option_count,
    o.has_withheld_counts,
    o.favour_votes,
    o.against_votes,
    o.abstain_votes,
    o.other_votes,
    (o.favour_options > 0 AND o.against_options > 0)             AS is_temperature_check,
    o.favour_votes + o.against_votes                             AS decisive_votes,
    -- NULL rather than 0 when there is nothing decisive to divide by, or when any count
    -- was withheld and the tally is therefore incomplete.
    if(o.has_withheld_counts, NULL,
       round(o.against_votes / nullIf(o.favour_votes + o.against_votes, 0), 4)
    )                                                            AS poll_against_share,

    -- Timing, from the host post.
    po.created_at                                                AS poll_posted_at

FROM opts AS o
INNER JOIN `dbt`.`stg_governance__forum_posts` AS po
    ON po.id = o.post_id