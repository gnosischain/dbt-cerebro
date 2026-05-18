

-- Per-group personal-token supply snapshot. One row per Group avatar.
--
-- "Supply" is the avatar's own personal CRC token in circulation, summed
-- across all holder categories (the same number `fct_execution_circles_v2_avatar_personal_token_supply_latest`
-- exposes). Filtered to avatar_type = 'Group' and joined to avatar_metadata
-- so the downstream leaderboard mart can render a profile cell without a
-- second join.

WITH groups AS (
    SELECT DISTINCT lower(avatar) AS avatar
    FROM `dbt`.`int_execution_circles_v2_avatars`
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
    coalesce(nullIf(m.metadata_name, ''), m.name) AS display_name,
    m.metadata_preview_image_url              AS preview_image_url
FROM groups g
LEFT JOIN `dbt`.`fct_execution_circles_v2_avatar_personal_token_supply_latest` s
    ON s.avatar = g.avatar
LEFT JOIN `dbt`.`api_execution_circles_v2_avatar_metadata` m
    ON m.avatar = g.avatar