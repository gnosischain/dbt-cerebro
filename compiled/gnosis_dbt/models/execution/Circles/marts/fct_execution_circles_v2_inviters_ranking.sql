

-- Leaderboard of top inviters by humans invited. Pre-joins the inviter's
-- IPFS profile (display name + preview image) and the current blacklist
-- flag so the dashboard can render the table without further joins.
--
-- join_use_nulls = 1 in the pre-hook so LEFT JOIN misses return NULL
-- (not the empty-string default), letting `b.address IS NOT NULL`
-- correctly drive the is_blacklisted flag.

WITH ranked AS (
    SELECT
        invited_by,
        count()                                              AS invite_count,
        min(block_timestamp)                                 AS first_invite_ts,
        max(block_timestamp)                                 AS last_invite_ts,
        row_number() OVER (ORDER BY count() DESC)            AS rank
    FROM `dbt`.`int_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
    GROUP BY invited_by
)

SELECT
    r.rank                                                   AS rank,
    r.invited_by                                             AS inviter,
    coalesce(m.metadata_name, '')                            AS display_name,
    coalesce(m.metadata_preview_image_url, '')               AS preview_image_url,
    b.address IS NOT NULL                                    AS is_blacklisted,
    r.invite_count                                           AS invite_count,
    r.first_invite_ts                                        AS first_invite_ts,
    r.last_invite_ts                                         AS last_invite_ts
FROM ranked r
LEFT JOIN `dbt`.`int_execution_circles_v2_avatar_metadata` m
       ON m.avatar = r.invited_by
LEFT JOIN `dbt`.`stg_crawlers_data__circles_blacklisted` b
       ON b.address = r.invited_by
ORDER BY rank