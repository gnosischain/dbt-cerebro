{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    topic_id,
    post_number,
    user_id,
    username,
    created_at,
    updated_at,
    reply_to_post_number,
    reply_count,
    reads,
    like_count,
    -- Snapshot proposal id (0x form) linked from the post body, when present —
    -- the second bridge (besides GIP number) from discussion to the vote.
    nullIf(extract(cooked, 'proposal/(0x[0-9a-fA-F]{64})'), '') AS snapshot_proposal_id,
    cooked,
    ingested_at
FROM {{ source('governance', 'forum_posts') }} FINAL
