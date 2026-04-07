{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='avatar',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET allow_experimental_json_type = 1"],
        tags=['production', 'execution', 'circles_v2', 'avatar_metadata']
    )
}}

-- Current parsed IPFS metadata for every Circles v2 avatar.
-- Joins the latest known (avatar, metadata_digest) pair from
-- `int_execution_circles_v2_avatar_metadata_targets` to the most
-- recently fetched payload in `circles_avatar_metadata_raw` and
-- exposes a small set of typed fields for downstream models.

WITH current_target AS (
    SELECT *
    FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }}
    WHERE is_current_avatar_metadata
),
latest_raw AS (
    SELECT
        avatar,
        metadata_digest,
        http_status,
        content_type,
        body,
        fetched_at,
        row_number() OVER (
            PARTITION BY avatar, metadata_digest
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ source('auxiliary', 'circles_avatar_metadata_raw') }}
    WHERE http_status = 200
      AND body != ''
)
SELECT
    a.avatar                                            AS avatar,
    a.avatar_type                                       AS avatar_type,
    a.name                                              AS onchain_name,
    t.metadata_digest                                   AS metadata_digest,
    t.ipfs_cid_v0                                       AS ipfs_cid_v0,
    t.gateway_url                                       AS gateway_url,
    JSONExtractString(r.body, 'name')                   AS metadata_name,
    JSONExtractString(r.body, 'symbol')                 AS metadata_symbol,
    JSONExtractString(r.body, 'description')            AS metadata_description,
    JSONExtractString(r.body, 'imageUrl')               AS metadata_image_url,
    JSONExtractString(r.body, 'previewImageUrl')        AS metadata_preview_image_url,
    r.body                                              AS metadata_body,
    r.fetched_at                                        AS metadata_fetched_at
FROM {{ ref('int_execution_circles_v2_avatars') }} a
LEFT JOIN current_target t
    ON a.avatar = t.avatar
LEFT JOIN latest_raw r
    ON t.avatar = r.avatar
   AND t.metadata_digest = r.metadata_digest
   AND r.rn = 1
