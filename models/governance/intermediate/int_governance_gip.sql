{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(gip_number)',
    tags=['production','governance','gip']
  )
}}

-- The GIP spine: one row per GIP number, unifying forum discussion with the
-- Snapshot vote. GIP universe = union of GIP numbers seen on either side
-- (forum ~148, Snapshot ~121, ~120 overlap). `universe` reads the base models
-- directly (not the forum/prop CTEs) to avoid ClickHouse WITH-reuse scoping
-- issues, and the key is aliased assumeNotNull so it is a valid, non-Nullable
-- MergeTree sort key. Unmatched LEFT JOIN fills default to 0/'' (correct for
-- the flags/counts); one-sided datetimes are nullIf'd off the 1970 sentinel.
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
    WHERE gip_number IS NOT NULL
    GROUP BY gip_number
),
prop AS (
    SELECT
        gip_number,
        argMax(id, created_at)            AS proposal_id,
        argMax(state, created_at)         AS proposal_state,
        argMax(outcome, created_at)       AS outcome,
        argMax(category, created_at)      AS category,
        argMax(created_at, created_at)    AS proposal_created_at,
        argMax(unique_voters, created_at) AS unique_voters,
        argMax(total_vp, created_at)      AS total_vp,
        argMax(scores_total, created_at)  AS scores_total
    FROM {{ ref('int_governance_proposals') }}
    WHERE gip_number IS NOT NULL
    GROUP BY gip_number
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
    p.proposal_id,
    p.proposal_state,
    p.outcome,
    p.category,
    nullIf(p.proposal_created_at, toDateTime('1970-01-01 00:00:00', 'UTC')) AS proposal_created_at,
    coalesce(p.unique_voters, 0)                                            AS unique_voters,
    coalesce(p.total_vp, 0.0)                                               AS total_vp,
    coalesce(p.scores_total, 0.0)                                           AS scores_total,
    coalesce(f.forum_topics, 0) > 0                                         AS discussed_on_forum,
    coalesce(p.proposal_id, '') != ''                                       AS reached_vote
FROM universe u
LEFT JOIN forum f ON u.gip_number = f.gip_number
LEFT JOIN prop  p ON u.gip_number = p.gip_number
