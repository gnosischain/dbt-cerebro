

-- Every forum post placed in the lifecycle phase of a proposal its topic is linked to:
-- pre_discussion / pre_vote / voting / post_close. This is what turns "opinion moved at
-- hour 40" into "here is what was being said at hour 40".
--
-- GRAIN IS (post, proposal), NOT post. A post in a topic linked to two proposals -- which
-- happens whenever a GIP is re-balloted -- yields two rows, and its phase can legitimately
-- differ between them (during ballot 2's vote, but after ballot 1 closed). Counting posts
-- from this model without grouping by proposal_id will double-count. That is deliberate:
-- collapsing to one phase per post would require inventing a canonical proposal, and the
-- redo cases are exactly where that invention would be wrong.
--
-- Coverage is partial and must be reported, never assumed: the bridge reaches 137 of 253
-- proposals (54%), effectively all GIP proposals. Posts in unlinked topics are absent
-- entirely -- this model answers "when was this said relative to a vote", so a post with
-- no vote to be relative to has no row.

WITH posts AS (
    SELECT
        id,
        topic_id,
        post_number,
        user_id,
        username,
        created_at,
        reply_to_post_number,
        reply_count,
        reads,
        like_count,
        -- Quote blocks are stripped before any text signal is derived. 11% of posts
        -- contain [quote=...] blocks holding SOMEONE ELSE'S words, and sampled cases
        -- showed glowing quoted text wrapped in a skeptical reply -- so counting a
        -- question mark or a character inside a quotation attributes it to the wrong
        -- author. This is the deterministic half of the quoted-opinion problem, and it
        -- costs one regex.
        replaceRegexpAll(raw, '(?s)\\[quote[^\\]]*\\].*?\\[/quote\\]', '') AS body_ex_quotes
    FROM `dbt`.`stg_governance__forum_posts`
)

SELECT
    po.id                       AS post_id,
    -- Aliased because topic_id also exists on the links relation; unaliased, ClickHouse
    -- emits the qualified name "po.topic_id" as the column name.
    po.topic_id                 AS topic_id,
    po.post_number,
    po.user_id,
    po.username,
    po.created_at               AS posted_at,
    po.reply_to_post_number,
    po.reply_count,
    po.reads,
    -- Current-state scalar, frozen at the topic's last bump. For like counts that can
    -- actually be attributed to a phase, use stg_governance__forum_likes, which carries
    -- each like's own timestamp.
    po.like_count               AS post_like_count_current,

    -- A question is a friction proxy that needs no sentiment model: it marks a post
    -- asking something rather than asserting something. Measured on quote-stripped text.
    position(po.body_ex_quotes, '?') > 0    AS has_question,
    length(po.body_ex_quotes)               AS body_chars_ex_quotes,
    -- Procedural one-liners ("cc @x", moderation notes) are 19% of the corpus and should
    -- not count as deliberation. Threshold matches the corpus profile, not a guess.
    length(po.body_ex_quotes) < 120         AS is_short_procedural,

    l.proposal_id,
    l.via_gip_number,
    l.via_post_link,
    pr.gip_number,
    pr.is_gip,
    pr.outcome,
    pr.created_at               AS proposal_created_at,
    pr.start_at,
    pr.end_at,

    
multiIf(
    po.created_at <  pr.created_at, 'pre_discussion',
    po.created_at <  pr.start_at,   'pre_vote',
    po.created_at <  pr.end_at,     'voting',
    'post_close'
) AS phase,

    -- Hours from the opening of voting; negative before it opened. Lets a post be aligned
    -- directly against api_governance_vote_trajectory.hour_offset to find which posts sit
    -- next to an inflection in the vote.
    dateDiff('hour', pr.start_at, po.created_at) AS hours_from_vote_open

FROM posts AS po
INNER JOIN `dbt`.`int_governance_proposal_topic_links` AS l
    ON l.topic_id = po.topic_id
INNER JOIN `dbt`.`int_governance_proposals` AS pr
    ON pr.id = l.proposal_id