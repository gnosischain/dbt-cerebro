

-- One row per (post, poll, option) with its vote count and a direction.
--
-- option_votes = -1 means WITHHELD, never zero. Discourse omits per-option counts while
-- a poll's results policy hides them (results = on_vote / on_close / staff_only), and 0
-- is a real zero. Mapped to NULL here so a sum cannot silently read a hidden live
-- temperature check as unanimous-nothing. Downstream must treat NULL as unknown, not 0.
--
-- Direction reuses classify_choice_polarity -- the SAME macro that labels Snapshot
-- choices. That is the point: a forum poll's "In Favour" and a ballot's "For" have to mean
-- the same thing for the poll-vs-vote comparison to be honest. The macro already handles
-- "in favou?r" (both spellings appear: 73 "In Favour", 2 "In Favor").
--
-- option_html is rendered HTML, so tags are stripped BEFORE matching. Without that an
-- anchor like <a href="...before...">x</a> can false-match a direction keyword that never
-- appears in the visible option text.
--
-- close_at stays Nullable and is a SCHEDULE, not an observation: Discourse never
-- reconciles it with reality, so 72 of 106 open polls carry an already-lapsed close_at and
-- 34 set none. `status` is the only open/closed signal -- never infer closure from
-- close_at.

SELECT
    post_id,
    topic_id,
    poll_id,
    poll_name,
    poll_type,
    status,
    results_visibility,
    is_public,
    close_at,
    voters,
    option_id,
    option_html,
    -- Visible option text, tags removed. The join key for direction, and what a UI shows.
    extractTextFromHTML(option_html)                                  AS option_text,
    
multiIf(
    match(lower(extractTextFromHTML(option_html)), '^abstain'), 'abstain',
    match(lower(extractTextFromHTML(option_html)), '(\\bagainst\\b|\\bno\\b|\\bnay\\b|reject|make no change|do not|\\bdon.?t\\b|do nothing|status quo|not now|\\bnone\\b)'), 'against',
    match(lower(extractTextFromHTML(option_html)), '(\\bfor\\b|\\byes\\b|approve|adopt|enact|accept|in favou?r|\\baye\\b|agree|support|let.?s do|proceed|enable|extend|launch|activate|ratify)'), 'for',
    'other'
) AS option_polarity,
    -- -1 (withheld) becomes NULL; genuine zeros survive as 0.
    nullIf(option_votes, -1)                                          AS option_votes,
    option_votes = -1                                                 AS votes_withheld,
    ingested_at
FROM `governance_db`.`forum_polls` FINAL