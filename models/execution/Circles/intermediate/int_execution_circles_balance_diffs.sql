{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, account, token_address)',
        unique_key='(version, transaction_hash, log_index, batch_index, account, token_address, token_id, delta_sign)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'balances']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    version,
    from_address AS account,
    to_address AS counterparty,
    token_id,
    token_address,
    -toInt256(amount_raw) AS delta_raw,
    -1 AS delta_sign,
    transfer_type
FROM {{ ref('int_execution_circles_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    version,
    to_address AS account,
    from_address AS counterparty,
    token_id,
    token_address,
    toInt256(amount_raw) AS delta_raw,
    1 AS delta_sign,
    transfer_type
FROM {{ ref('int_execution_circles_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
