{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    lower(follower) AS follower,
    space_id,
    created_at,
    ingested_at
FROM {{ source('governance', 'snapshot_follows') }} FINAL
