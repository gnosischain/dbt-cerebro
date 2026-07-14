{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='group_avatar',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'groups', 'supply', 'snapshot']
    )
}}

-- Per-group personal-token supply snapshot. One row per Group avatar.
--
-- "Supply" is the avatar's own personal CRC token in circulation, summed
-- across all holder categories (the same number `fct_execution_circles_v2_avatar_personal_token_supply_latest`
-- exposes). Filtered to avatar_type = 'Group' and joined to avatar_metadata
-- so the downstream leaderboard mart can render a profile cell without a
-- second join.

WITH groups AS (
    SELECT DISTINCT lower(avatar) AS avatar
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Group'
)

SELECT
    g.avatar                                  AS group_avatar,
    coalesce(s.supply, toFloat64(0))          AS supply,
    coalesce(s.wrapped, toFloat64(0))         AS wrapped,
    coalesce(s.unwrapped, toFloat64(0))       AS unwrapped,
    s.wrapped_pct                             AS wrapped_pct,
    coalesce(s.supply_demurraged, toFloat64(0))   AS supply_demurraged,
    coalesce(s.wrapped_demurraged, toFloat64(0))  AS wrapped_demurraged,
    m.display_name                            AS display_name,
    m.preview_image_url                       AS preview_image_url
FROM groups g
LEFT JOIN {{ ref('fct_execution_circles_v2_avatar_personal_token_supply_latest') }} s
    ON s.avatar = g.avatar
-- Dedupe avatar_metadata (a few avatars carry >1 registration row, which
-- otherwise duplicates the group in the downstream leaderboard).
LEFT JOIN (
    SELECT
        avatar,
        argMax(display_name, registered_at)      AS display_name,
        argMax(preview_image_url, registered_at) AS preview_image_url
    FROM (
        SELECT
            lower(avatar) AS avatar,
            coalesce(nullIf(metadata_name, ''), name) AS display_name,
            metadata_preview_image_url AS preview_image_url,
            registered_at
        FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
    )
    GROUP BY avatar
) m ON m.avatar = g.avatar
