{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'avatars']
    )
}}

WITH latest_avatar_event AS (
    SELECT
        avatar,
        argMax(version, tuple(block_number, transaction_index, log_index)) AS version,
        argMax(avatar_type, tuple(block_number, transaction_index, log_index)) AS avatar_type,
        argMax(invited_by, tuple(block_number, transaction_index, log_index)) AS invited_by,
        argMax(token_id, tuple(block_number, transaction_index, log_index)) AS token_id,
        argMax(name, tuple(block_number, transaction_index, log_index)) AS name,
        argMax(block_number, tuple(block_number, transaction_index, log_index)) AS block_number,
        argMax(block_timestamp, tuple(block_number, transaction_index, log_index)) AS block_timestamp,
        argMax(transaction_hash, tuple(block_number, transaction_index, log_index)) AS transaction_hash,
        argMax(transaction_index, tuple(block_number, transaction_index, log_index)) AS transaction_index,
        argMax(log_index, tuple(block_number, transaction_index, log_index)) AS log_index
    FROM {{ ref('int_execution_circles_avatars') }}
    GROUP BY 1
),
latest_metadata AS (
    SELECT
        lower(decoded_params['avatar']) AS avatar,
        argMax(decoded_params['metadataDigest'], tuple(block_number, transaction_index, log_index)) AS cid_v0_digest
    FROM {{ ref('contracts_circles_v2_NameRegistry_events') }}
    WHERE event_name = 'UpdateMetadataDigest'
    GROUP BY 1
)

SELECT
    a.block_number,
    a.block_timestamp,
    a.transaction_hash,
    a.transaction_index,
    a.log_index,
    a.version,
    a.avatar_type,
    a.invited_by,
    a.avatar,
    a.token_id,
    a.name,
    m.cid_v0_digest
FROM latest_avatar_event a
LEFT JOIN latest_metadata m
    ON a.avatar = m.avatar
