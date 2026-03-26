{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, transaction_hash, log_index, batch_index)',
        unique_key='(version, transaction_hash, log_index, batch_index, token_address, token_id)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'transfers']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    1 AS version,
    0 AS batch_index,
    CAST(NULL AS Nullable(String)) AS operator,
    from_address AS from_address,
    to_address AS to_address,
    token_address AS token_id,
    amount_raw,
    token_address,
    'CrcV1_Transfer' AS transfer_type
FROM {{ ref('int_execution_circles_v1_token_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    2 AS version,
    batch_index,
    operator,
    from_address,
    to_address,
    token_id,
    amount_raw,
    token_address,
    transfer_type
FROM {{ ref('int_execution_circles_v2_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    2 AS version,
    0 AS batch_index,
    CAST(NULL AS Nullable(String)) AS operator,
    from_address,
    to_address,
    token_address AS token_id,
    amount_raw,
    token_address,
    'CrcV2_ERC20WrapperTransfer' AS transfer_type
FROM {{ ref('int_execution_circles_wrapper_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
