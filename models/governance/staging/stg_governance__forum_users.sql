{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    username,
    name,
    trust_level,
    likes_received,
    likes_given,
    post_count,
    topic_count,
    days_visited,
    ingested_at
FROM {{ source('governance', 'forum_users') }} FINAL
