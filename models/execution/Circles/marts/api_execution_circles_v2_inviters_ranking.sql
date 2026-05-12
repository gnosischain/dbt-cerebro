{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_inviters_ranking','granularity:latest']
  )
}}

-- Leaderboard of top inviters by humans invited. Passthrough over
-- fct_execution_circles_v2_inviters_ranking, which pre-joins the inviter's
-- display name, preview image URL, and current blacklist flag.

SELECT
    rank,
    inviter,
    display_name,
    preview_image_url,
    is_blacklisted,
    invite_count,
    first_invite_ts,
    last_invite_ts
FROM {{ ref('fct_execution_circles_v2_inviters_ranking') }}
ORDER BY rank
