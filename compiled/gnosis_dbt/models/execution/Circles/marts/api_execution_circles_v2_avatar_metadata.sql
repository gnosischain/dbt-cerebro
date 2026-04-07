

-- Per-avatar identity for Circles v2: on-chain registration metadata
-- joined to the most recently fetched IPFS profile (display name,
-- preview/image URL, description). Backs the "Avatar Identity" panel
-- on the Circles dashboard.

SELECT
    a.avatar,
    a.avatar_type,
    a.invited_by,
    a.name,
    a.token_id,
    a.block_timestamp                  AS registered_at,
    m.metadata_digest                  AS current_metadata_digest,
    m.ipfs_cid_v0                      AS current_ipfs_cid_v0,
    m.gateway_url                      AS current_gateway_url,
    m.metadata_name                    AS metadata_name,
    m.metadata_symbol                  AS metadata_symbol,
    m.metadata_description             AS metadata_description,
    m.metadata_image_url               AS metadata_image_url,
    m.metadata_preview_image_url       AS metadata_preview_image_url,
    m.metadata_fetched_at              AS metadata_fetched_at
FROM `dbt`.`int_execution_circles_v2_avatars` a
LEFT JOIN `dbt`.`int_execution_circles_v2_avatar_metadata` m
    ON a.avatar = m.avatar
WHERE a.avatar IS NOT NULL