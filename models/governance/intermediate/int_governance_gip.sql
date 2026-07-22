{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(gip_number)',
    tags=['production','governance','gip']
  )
}}

-- The GIP spine: one row per GIP number, unifying forum discussion with
-- Snapshot proposals. GIP universe = union of numbers seen on either side.
--
-- Outcome is CANONICAL when unambiguous, else NULL:
--   * exactly one decisive outcome among proposals (passed/rejected/decided/
--     no_consensus), ignoring below_quorum/open siblings → that outcome
--   * several decisive outcomes that disagree (number reuse / conflicting
--     redos) → NULL + outcome_ambiguous
--   * only non-decisive outcomes, all equal → that outcome (e.g. below_quorum)
-- Latest ballot is always exposed separately as latest_* (never pretend it is
-- "the" GIP outcome when history conflicts).
--
-- `universe` reads base models directly (not the forum/prop CTEs) to avoid
-- ClickHouse WITH-reuse scoping issues. Spine key is assumeNotNull for a
-- valid MergeTree sort key. Unmatched LEFT JOIN datetimes are nullIf'd off
-- the 1970 sentinel.
WITH forum AS (
    SELECT
        gip_number,
        count()                     AS forum_topics,
        sum(posts_count)            AS forum_posts,
        sum(views)                  AS forum_views,
        sum(reply_count)            AS forum_replies,
        max(participant_count)      AS max_participants,
        max(phase = 'phase-1')      AS has_phase1,
        max(phase = 'phase-2')      AS has_phase2,
        max(phase = 'phase-3')      AS has_phase3,
        min(created_at)             AS first_forum_at,
        max(bumped_at)              AS last_forum_at,
        argMax(id, posts_count)     AS primary_topic_id,
        argMax(title, posts_count)  AS primary_topic_title
    FROM {{ ref('stg_governance__forum_topics') }}
    WHERE gip_number > 0
    GROUP BY gip_number
),
prop_base AS (
    SELECT
        gip_number,
        count() AS proposal_count,
        max(outcome = 'passed')       AS has_passed,
        max(outcome = 'rejected')     AS has_rejected,
        max(outcome = 'decided')      AS has_decided,
        max(outcome = 'no_consensus') AS has_no_consensus,
        max(outcome = 'below_quorum') AS has_below_quorum,
        groupUniqArrayIf(
            outcome,
            outcome IN ('passed', 'rejected', 'decided', 'no_consensus')
        ) AS decisive_outcomes,
        groupUniqArray(outcome) AS all_outcomes,
        argMax(id, created_at)            AS latest_proposal_id,
        argMax(outcome, created_at)       AS latest_outcome,
        argMax(state, created_at)         AS latest_proposal_state,
        argMax(category, created_at)      AS latest_category,
        argMax(created_at, created_at)    AS latest_proposal_created_at,
        argMax(unique_voters, created_at) AS latest_unique_voters,
        argMax(total_vp, created_at)      AS latest_total_vp,
        argMax(scores_total, created_at)  AS latest_scores_total
    FROM {{ ref('int_governance_proposals') }}
    WHERE gip_number > 0
    GROUP BY gip_number
),
prop_resolved AS (
    SELECT
        b.*,
        length(b.decisive_outcomes) > 1 AS outcome_ambiguous,
        multiIf(
            length(b.decisive_outcomes) = 1, b.decisive_outcomes[1],
            length(b.decisive_outcomes) > 1, CAST(NULL AS Nullable(String)),
            length(b.all_outcomes) = 1,      b.all_outcomes[1],
            CAST(NULL AS Nullable(String))
        ) AS outcome
    FROM prop_base AS b
),
-- Attributes of the canonical proposal (latest row matching canonical outcome).
-- When outcome is ambiguous/NULL, proposal_id stays empty; use latest_* instead.
prop AS (
    SELECT
        r.gip_number,
        r.proposal_count,
        r.has_passed,
        r.has_rejected,
        r.has_decided,
        r.has_no_consensus,
        r.has_below_quorum,
        r.outcome_ambiguous,
        r.outcome,
        r.latest_proposal_id,
        r.latest_outcome,
        r.latest_proposal_state,
        r.latest_category,
        r.latest_proposal_created_at,
        r.latest_unique_voters,
        r.latest_total_vp,
        r.latest_scores_total,
        if(
            r.outcome IS NULL,
            '',
            argMax(p.id, p.created_at)
        ) AS proposal_id,
        if(
            r.outcome IS NULL,
            '',
            argMax(p.state, p.created_at)
        ) AS proposal_state,
        if(
            r.outcome IS NULL,
            '',
            argMax(p.category, p.created_at)
        ) AS category,
        if(
            r.outcome IS NULL,
            toDateTime('1970-01-01 00:00:00', 'UTC'),
            argMax(p.created_at, p.created_at)
        ) AS proposal_created_at,
        if(
            r.outcome IS NULL,
            toUInt64(0),
            argMax(p.unique_voters, p.created_at)
        ) AS unique_voters,
        if(
            r.outcome IS NULL,
            toFloat64(0),
            argMax(p.total_vp, p.created_at)
        ) AS total_vp,
        if(
            r.outcome IS NULL,
            toFloat64(0),
            argMax(p.scores_total, p.created_at)
        ) AS scores_total
    FROM prop_resolved AS r
    LEFT JOIN {{ ref('int_governance_proposals') }} AS p
        ON r.gip_number = p.gip_number
        AND r.outcome IS NOT NULL
        AND p.outcome = r.outcome
    GROUP BY
        r.gip_number,
        r.proposal_count,
        r.has_passed,
        r.has_rejected,
        r.has_decided,
        r.has_no_consensus,
        r.has_below_quorum,
        r.outcome_ambiguous,
        r.outcome,
        r.latest_proposal_id,
        r.latest_outcome,
        r.latest_proposal_state,
        r.latest_category,
        r.latest_proposal_created_at,
        r.latest_unique_voters,
        r.latest_total_vp,
        r.latest_scores_total
),
universe AS (
    SELECT gip_number FROM {{ ref('stg_governance__forum_topics') }} WHERE gip_number IS NOT NULL
    UNION DISTINCT
    SELECT gip_number FROM {{ ref('int_governance_proposals') }} WHERE gip_number IS NOT NULL
)
SELECT
    assumeNotNull(u.gip_number)                                             AS gip_number,
    coalesce(f.forum_topics, 0)                                             AS forum_topics,
    coalesce(f.forum_posts, 0)                                              AS forum_posts,
    coalesce(f.forum_views, 0)                                              AS forum_views,
    coalesce(f.forum_replies, 0)                                            AS forum_replies,
    coalesce(f.max_participants, 0)                                         AS max_participants,
    coalesce(f.has_phase1, 0)                                               AS has_phase1,
    coalesce(f.has_phase2, 0)                                               AS has_phase2,
    coalesce(f.has_phase3, 0)                                               AS has_phase3,
    nullIf(f.first_forum_at, toDateTime('1970-01-01 00:00:00', 'UTC'))      AS first_forum_at,
    nullIf(f.last_forum_at,  toDateTime('1970-01-01 00:00:00', 'UTC'))      AS last_forum_at,
    f.primary_topic_id,
    f.primary_topic_title,
    coalesce(p.proposal_count, 0)                                           AS proposal_count,
    coalesce(p.has_passed, 0)                                               AS has_passed,
    coalesce(p.has_rejected, 0)                                             AS has_rejected,
    coalesce(p.has_decided, 0)                                              AS has_decided,
    coalesce(p.has_no_consensus, 0)                                         AS has_no_consensus,
    coalesce(p.has_below_quorum, 0)                                         AS has_below_quorum,
    coalesce(p.outcome_ambiguous, 0)                                        AS outcome_ambiguous,
    p.outcome,
    nullIf(p.proposal_id, '')                                               AS proposal_id,
    nullIf(p.proposal_state, '')                                            AS proposal_state,
    nullIf(p.category, '')                                                  AS category,
    nullIf(p.proposal_created_at, toDateTime('1970-01-01 00:00:00', 'UTC')) AS proposal_created_at,
    coalesce(p.unique_voters, 0)                                            AS unique_voters,
    coalesce(p.total_vp, 0.0)                                               AS total_vp,
    coalesce(p.scores_total, 0.0)                                           AS scores_total,
    nullIf(p.latest_proposal_id, '')                                        AS latest_proposal_id,
    nullIf(p.latest_outcome, '')                                            AS latest_outcome,
    coalesce(f.forum_topics, 0) > 0                                         AS discussed_on_forum,
    coalesce(p.proposal_count, 0) > 0                                       AS reached_vote
FROM universe u
LEFT JOIN forum f ON u.gip_number = f.gip_number
LEFT JOIN prop  p ON u.gip_number = p.gip_number
-- Defensive: never emit a 0/NULL spine key (guards a ClickHouse UNION-over-
-- Nullable quirk that can surface a NULL slot -> assumeNotNull -> 0).
WHERE u.gip_number > 0
