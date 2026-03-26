{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, token)',
        unique_key='(version, transaction_hash, log_index, token)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'tokens']
    )
}}

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    version,
    concat('AvatarToken_', avatar_type) AS token_type,
    token_id AS token,
    avatar AS token_owner,
    avatar AS avatar,
    source_event_name AS source_event_name
FROM {{ ref('int_execution_circles_avatars') }}
WHERE token_id IS NOT NULL
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    2 AS version,
    CASE
        WHEN circles_type = 1 THEN 'CrcV2_ERC20WrapperDeployed_Inflationary'
        ELSE 'CrcV2_ERC20WrapperDeployed_Demurraged'
    END AS token_type,
    wrapper_address AS token,
    avatar AS token_owner,
    avatar,
    'ERC20WrapperDeployed' AS source_event_name
FROM {{ ref('int_execution_circles_wrappers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
