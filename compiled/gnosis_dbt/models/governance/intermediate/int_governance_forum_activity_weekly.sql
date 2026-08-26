

-- Weekly forum activity in long format (date, metric, value). The
-- created_at > 2015 filter drops the epoch sentinel used for missing
-- timestamps so it never appears as a 1970 bucket.
SELECT toStartOfWeek(created_at, 1) AS date, 'topics_created' AS metric, toUInt64(count()) AS value
FROM `dbt`.`stg_governance__forum_topics`
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfWeek(created_at, 1) AS date, 'posts_created' AS metric, toUInt64(count()) AS value
FROM `dbt`.`stg_governance__forum_posts`
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfWeek(created_at, 1) AS date, 'active_users' AS metric, toUInt64(uniqExact(user_id)) AS value
FROM `dbt`.`stg_governance__forum_posts`
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC') AND user_id > 0
GROUP BY date

UNION ALL

-- Likes eligibility is the canonical contract shared with the governance
-- mini-app: active (hidden=0, deleted=0) and mapped (target topic and post
-- still exist). Same filter in api_governance_discussion_phases.
SELECT toStartOfWeek(created_at, 1) AS date, 'likes_given' AS metric, toUInt64(count()) AS value
FROM `dbt`.`stg_governance__forum_likes`
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
  AND hidden = 0 AND deleted = 0
  AND topic_id IN (SELECT id FROM `dbt`.`stg_governance__forum_topics`)
  AND post_id IN (SELECT id FROM `dbt`.`stg_governance__forum_posts`)
GROUP BY date

UNION ALL

SELECT toStartOfWeek(created_at, 1) AS date, 'distinct_likers' AS metric, toUInt64(uniqExact(acting_user_id)) AS value
FROM `dbt`.`stg_governance__forum_likes`
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
  AND hidden = 0 AND deleted = 0
  AND topic_id IN (SELECT id FROM `dbt`.`stg_governance__forum_topics`)
  AND post_id IN (SELECT id FROM `dbt`.`stg_governance__forum_posts`)
GROUP BY date

UNION ALL

-- Poll grain is (post, poll_name); options collapse via max(voters), never a
-- sum over options. Polls are dated by their HOST POST (the poll row has no
-- created_at of its own).
SELECT toStartOfWeek(po.created_at, 1) AS date, 'polls_created' AS metric, toUInt64(count()) AS value
FROM (
    SELECT post_id, poll_name
    FROM `dbt`.`stg_governance__forum_polls`
    GROUP BY post_id, poll_name
) AS pl
INNER JOIN `dbt`.`stg_governance__forum_posts` AS po ON po.id = pl.post_id
WHERE po.created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfWeek(po.created_at, 1) AS date, 'poll_voter_slots' AS metric, toUInt64(sum(pl.voters)) AS value
FROM (
    SELECT post_id, poll_name, max(voters) AS voters
    FROM `dbt`.`stg_governance__forum_polls`
    GROUP BY post_id, poll_name
) AS pl
INNER JOIN `dbt`.`stg_governance__forum_posts` AS po ON po.id = pl.post_id
WHERE po.created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date