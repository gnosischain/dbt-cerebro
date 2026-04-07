{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_metadata_history','granularity:history']
    )
}}

-- Historical timeline of every Circles v2 avatar metadata change.
-- One row per (avatar, metadata_digest) ever announced, with
-- valid_from / valid_to / is_current so the dashboard can render
-- the full sequence of name + image updates per avatar.

SELECT
    avatar,
    avatar_type,
    onchain_name,
    metadata_digest,
    ipfs_cid_v0,
    gateway_url,
    valid_from,
    valid_to,
    is_current,
    transaction_hash,
    log_index,
    metadata_name,
    metadata_symbol,
    metadata_description,
    metadata_image_url,
    metadata_preview_image_url,
    metadata_fetched_at
FROM {{ ref('int_execution_circles_v2_avatar_metadata_history') }}
WHERE avatar IS NOT NULL
