{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    parent_id,
    name,
    slug,
    topic_count,
    post_count,
    description,
    ingested_at
FROM {{ source('governance', 'forum_categories') }} FINAL
