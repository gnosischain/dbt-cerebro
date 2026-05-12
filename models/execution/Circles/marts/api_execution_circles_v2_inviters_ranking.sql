{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_inviters_ranking','granularity:latest']
  )
}}

-- Leaderboard of top inviters by number of human avatars invited. Thin
-- passthrough over fct_execution_circles_v2_inviters_ranking; ordering
-- preserves the fact's pre-computed `rank` column.

SELECT
    rank,
    invited_by         AS inviter,
    invite_count,
    first_invite_ts,
    last_invite_ts
FROM {{ ref('fct_execution_circles_v2_inviters_ranking') }}
ORDER BY rank
