{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, transaction_hash, log_index, batch_index)',
        unique_key='(version, transaction_hash, log_index, batch_index)',
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
    batch_index,
    version,
    from_address AS "from",
    to_address AS "to",
    amount_raw AS value,
    toJSONString(
        map(
            'transfer_type', transfer_type,
            'token_id', token_id,
            'token_address', token_address
        )
    ) AS events
FROM {{ ref('int_execution_circles_transfers') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
