

-- Top 200 forum contributors by posts written, keyed by an irreversible
-- salted pseudonym (pseudonymize_address over the Discourse user_id) -- no
-- username, no raw id, per the governance privacy decision. Likes use the
-- canonical eligibility contract (active + mapped). Missing like counts read
-- as 0 (ClickHouse default LEFT JOIN semantics, wanted here).

WITH eligible_likes AS (
    SELECT post_id, acting_user_id, created_at
    FROM `dbt`.`stg_governance__forum_likes`
    WHERE hidden = 0 AND deleted = 0
      AND topic_id IN (SELECT id FROM `dbt`.`stg_governance__forum_topics`)
      AND post_id IN (SELECT id FROM `dbt`.`stg_governance__forum_posts`)
),

posts AS (
    SELECT
        user_id,
        count()                     AS posts,
        countIf(post_number = 1)    AS topics_started,
        min(created_at)             AS first_post_at,
        max(created_at)             AS last_post_at
    FROM `dbt`.`stg_governance__forum_posts`
    WHERE user_id > 0
      AND created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
    GROUP BY user_id
),

likes_given AS (
    SELECT acting_user_id AS user_id, count() AS likes_given
    FROM eligible_likes
    GROUP BY acting_user_id
),

likes_received AS (
    SELECT po.user_id AS user_id, count() AS likes_received
    FROM eligible_likes AS l
    INNER JOIN `dbt`.`stg_governance__forum_posts` AS po ON po.id = l.post_id
    WHERE po.user_id > 0
    GROUP BY po.user_id
)

SELECT
    sub.contributor_key AS contributor_key,
    sub.posts AS posts,
    sub.topics_started AS topics_started,
    sub.likes_received AS likes_received,
    sub.likes_given AS likes_given,
    sub.trust_level AS trust_level,
    sub.first_post_at AS first_post_at,
    sub.last_post_at AS last_post_at,
    (SELECT toDate(max(created_at)) FROM `dbt`.`stg_governance__forum_posts`) AS as_of_date
FROM (
    SELECT
        
    sipHash64(concat(unhex('00'), lower(toString(p.user_id))))
 AS contributor_key,
        p.posts AS posts,
        p.topics_started AS topics_started,
        lr.likes_received AS likes_received,
        lg.likes_given AS likes_given,
        u.trust_level AS trust_level,
        p.first_post_at AS first_post_at,
        p.last_post_at AS last_post_at
    FROM posts AS p
    LEFT JOIN likes_received AS lr ON lr.user_id = p.user_id
    LEFT JOIN likes_given AS lg ON lg.user_id = p.user_id
    LEFT JOIN `dbt`.`stg_governance__forum_users` AS u ON u.id = p.user_id
    ORDER BY p.posts DESC, p.user_id
    LIMIT 200
) AS sub