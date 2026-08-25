{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

-- One row per (post, liker) with the like's own timestamp.
--
-- Why this is not just forum_posts.like_count: that column is a scalar captured at
-- fetch time and, because daily mode only re-reads a topic whose bumped_at beat the
-- watermark, it is frozen at the topic's last activity. It carries no timestamp and no
-- actor, so "how many likes did this post get DURING the voting window" is unanswerable
-- from it. Here every like has its own created_at, so phase attribution is exact rather
-- than approximated by the post's own date.
--
-- Hidden and deleted edges are kept, not filtered: dropping them here would silently
-- change like totals relative to forum_posts.like_count and make the two impossible to
-- reconcile. Filter downstream where the intent is explicit.

SELECT
    post_id,
    topic_id,
    post_number,
    acting_user_id,
    -- Lowercased for stable joins against forum_users.username; the raw value is in raw_json.
    lower(acting_username) AS acting_username,
    created_at,
    hidden,
    deleted,
    ingested_at
FROM {{ source('governance', 'forum_likes') }} FINAL
