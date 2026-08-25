{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, metric)',
    tags=['production','governance','forum']
  )
}}

-- Weekly forum activity in long format (date, metric, value). The
-- created_at > 2015 filter drops the epoch sentinel used for missing
-- timestamps so it never appears as a 1970 bucket.
SELECT toStartOfWeek(created_at, 1) AS date, 'topics_created' AS metric, toUInt64(count()) AS value
FROM {{ ref('stg_governance__forum_topics') }}
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfWeek(created_at, 1) AS date, 'posts_created' AS metric, toUInt64(count()) AS value
FROM {{ ref('stg_governance__forum_posts') }}
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC')
GROUP BY date

UNION ALL

SELECT toStartOfWeek(created_at, 1) AS date, 'active_users' AS metric, toUInt64(uniqExact(user_id)) AS value
FROM {{ ref('stg_governance__forum_posts') }}
WHERE created_at > toDateTime('2015-01-01 00:00:00', 'UTC') AND user_id > 0
GROUP BY date
