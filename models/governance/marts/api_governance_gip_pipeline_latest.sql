{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_gip_pipeline','granularity:latest']
  )
}}

-- The pending GIP pipeline: open forum topics whose GIP number has never
-- reached a Snapshot vote, with idle time. 45 idle days is the canonical
-- dormancy threshold shared with the governance mini-app; unlike the app's
-- pipeline panel (cap 24, dormant and phase-1 rows hidden behind counters),
-- every pending topic is exposed here with flags -- capping is presentation.

SELECT
    sub.topic_id AS topic_id,
    sub.title AS title,
    sub.gip_number AS gip_number,
    sub.phase AS phase,
    sub.posts_count AS posts_count,
    sub.participant_count AS participant_count,
    sub.views AS views,
    sub.created_at AS created_at,
    sub.last_posted_at AS last_posted_at,
    sub.days_idle AS days_idle,
    sub.is_dormant AS is_dormant,
    toDate(now()) AS as_of_date
FROM (
    SELECT
        t.id                                            AS topic_id,
        t.title                                         AS title,
        t.gip_number                                    AS gip_number,
        t.phase                                         AS phase,
        t.posts_count                                   AS posts_count,
        t.participant_count                             AS participant_count,
        t.views                                         AS views,
        t.created_at                                    AS created_at,
        t.last_posted_at                                AS last_posted_at,
        toInt64(dateDiff('day', t.last_posted_at, now())) AS days_idle,
        dateDiff('day', t.last_posted_at, now()) > 45   AS is_dormant
    FROM {{ ref('stg_governance__forum_topics') }} AS t
    INNER JOIN {{ ref('int_governance_gip') }} AS g
        ON g.gip_number = t.gip_number
    WHERE t.gip_number > 0
      AND g.reached_vote = 0
      AND t.closed = 0
      AND t.archived = 0
) AS sub
ORDER BY sub.days_idle ASC
