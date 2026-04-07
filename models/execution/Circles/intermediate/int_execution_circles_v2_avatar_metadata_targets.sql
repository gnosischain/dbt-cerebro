{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles_v2', 'avatar_metadata']
    )
}}

-- Deterministic queue of every (avatar, metadata_digest) pair the
-- Circles v2 NameRegistry has ever announced. Used by:
--   * scripts/circles/backfill_avatar_metadata.py (one-time)
--   * macros/circles/fetch_and_insert_circles_metadata.sql (nightly)
-- to know which IPFS payloads still need to be fetched.

WITH events AS (
    SELECT
        block_timestamp,
        lower(concat('0x', transaction_hash)) AS transaction_hash,
        log_index,
        lower(decoded_params['avatar']) AS avatar,
        decoded_params['metadataDigest'] AS metadata_digest
    FROM {{ ref('contracts_circles_v2_NameRegistry_events') }}
    WHERE event_name = 'UpdateMetadataDigest'
      AND decoded_params['metadataDigest'] != ''
      AND decoded_params['metadataDigest'] IS NOT NULL
),
dedup AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY avatar, metadata_digest
            ORDER BY block_timestamp DESC, log_index DESC
        ) AS rn_pair,
        row_number() OVER (
            PARTITION BY avatar
            ORDER BY block_timestamp DESC, log_index DESC
        ) AS rn_avatar
    FROM events
)
SELECT
    avatar,
    metadata_digest,
    {{ circles_metadata_digest_hex('metadata_digest') }} AS metadata_digest_hex,
    {{ circles_metadata_digest_to_cid_v0('metadata_digest') }} AS ipfs_cid_v0,
    {{ circles_metadata_gateway_url('metadata_digest') }} AS gateway_url,
    block_timestamp,
    transaction_hash,
    log_index,
    rn_avatar = 1 AS is_current_avatar_metadata
FROM dedup
WHERE rn_pair = 1
