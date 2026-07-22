{{
  config(
    materialized='view',
    tags=['production','governance','api:governance_forum_proposal_links','granularity:latest']
  )
}}

-- Forum discussion <-> Snapshot vote cross-links, topic grain. Two
-- independent signals, UNIONed (not just decorated onto one source) so a
-- link found by only ONE method still gets its own row:
--   via_forum_post: a post in the topic pastes the proposal's own 0x URL
--   via_discussion: the proposal's own authored `discussion` field (Snapshot's
--                   field, set by the proposal author) points at this topic
-- Verified: of 89 proposals with a parseable discussion_topic_id, 43 have NO
-- corroborating pasted-post link at all -- a real coverage gain, not
-- redundant with the post-link method. A handful of those are genuine
-- multi-row disagreements: a topic hosting evidence for two DIFFERENT
-- proposals (almost always a multi-ballot GIP redo -- e.g. GIP-134's original
-- (passed) and redo (below_quorum) BOTH point their own `discussion` at the
-- same topic 10392, but only the redo also got a link pasted back in a post).
-- An earlier version of this model only surfaced post-link-derived rows,
-- silently dropping every discussion-only pair.
--
-- recovered_via_link: the topic's own title has no GIP number but a linked
-- proposal does (post-link driven; 88 verified cases).
WITH post_derived AS (
    SELECT
        l.topic_id,
        l.snapshot_proposal_id,
        l.proposal_matched,
        l.proposal_id,
        l.proposal_gip_number,
        nullIf(l.proposal_title, '')   AS proposal_title,
        nullIf(l.proposal_state, '')   AS proposal_state,
        nullIf(l.proposal_outcome, '') AS proposal_outcome,
        l.linking_posts,
        CAST(l.first_linked_at AS Nullable(DateTime)) AS first_linked_at,
        CAST(l.last_linked_at  AS Nullable(DateTime)) AS last_linked_at,
        true AS via_forum_post
    FROM {{ ref('int_governance_forum_topic_proposal_links') }} l
),
discussion_derived AS (
    SELECT
        sp.discussion_topic_id AS topic_id,
        sp.id                  AS snapshot_proposal_id,
        true                    AS proposal_matched,
        sp.id                   AS proposal_id,
        sp.gip_number           AS proposal_gip_number,
        nullIf(sp.title, '')    AS proposal_title,
        nullIf(sp.state, '')    AS proposal_state,
        nullIf(ip.outcome, '')  AS proposal_outcome,
        toUInt64(0)             AS linking_posts,
        CAST(NULL AS Nullable(DateTime)) AS first_linked_at,
        CAST(NULL AS Nullable(DateTime)) AS last_linked_at,
        false AS via_forum_post
    FROM {{ ref('stg_governance__snapshot_proposals') }} sp
    LEFT JOIN {{ ref('int_governance_proposals') }} ip ON sp.id = ip.id
    WHERE sp.discussion_topic_id IS NOT NULL
),
-- Discussion-derived pairs not already found via a pasted post link.
-- LEFT ANTI JOIN, deliberately NOT "LEFT JOIN ... WHERE right.col IS NULL":
-- verified the latter is unreliable for this compound (topic_id, proposal_id)
-- key in this environment -- it silently matched on a partial key and
-- produced 0 anti-joined rows where 43 were expected. LEFT ANTI JOIN is the
-- correct, verified-working construct for this exact need.
discussion_only AS (
    SELECT d.* FROM discussion_derived d
    LEFT ANTI JOIN post_derived pd
        ON d.topic_id = pd.topic_id AND d.proposal_id = pd.proposal_id
),
combined AS (
    SELECT * FROM post_derived
    UNION ALL
    SELECT * FROM discussion_only
)
SELECT
    c.topic_id,
    t.title                                                       AS topic_title,
    t.gip_number                                                  AS topic_gip_number,
    t.phase                                                       AS topic_phase,
    c.snapshot_proposal_id,
    c.proposal_matched,
    c.proposal_id,
    c.proposal_gip_number,
    c.proposal_title,
    c.proposal_state,
    c.proposal_outcome,
    c.linking_posts,
    c.first_linked_at,
    c.last_linked_at,
    t.gip_number IS NULL AND c.proposal_gip_number IS NOT NULL     AS recovered_via_link,
    c.via_forum_post,
    coalesce(sp.discussion_topic_id = c.topic_id, false)          AS via_discussion,
    c.via_forum_post AND coalesce(sp.discussion_topic_id = c.topic_id, false) AS corroborated_by_discussion
FROM combined c
LEFT JOIN {{ ref('stg_governance__forum_topics') }} t ON c.topic_id = t.id
LEFT JOIN {{ ref('stg_governance__snapshot_proposals') }} sp ON c.proposal_id = sp.id
