{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index, batch_index)',
        unique_key='(transaction_hash, log_index, batch_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'transfers']
    )
}}

WITH single_rows AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        0 AS batch_index,
        lower(decoded_params['operator']) AS operator,
        lower(decoded_params['from']) AS from_address,
        lower(decoded_params['to']) AS to_address,
        toString(toUInt256OrZero(decoded_params['id'])) AS token_id,
        toUInt256OrZero(decoded_params['value']) AS amount_raw,
        lower(contract_address) AS token_address,
        'CrcV2_TransferSingle' AS transfer_type
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name = 'TransferSingle'
      {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
),
batch_rows AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        arrayEnumerate(JSONExtract(decoded_params['ids'], 'Array(String)')) AS batch_indexes,
        lower(decoded_params['operator']) AS operator,
        lower(decoded_params['from']) AS from_address,
        lower(decoded_params['to']) AS to_address,
        JSONExtract(decoded_params['ids'], 'Array(String)') AS token_ids,
        JSONExtract(decoded_params['values'], 'Array(String)') AS values,
        lower(contract_address) AS token_address
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name = 'TransferBatch'
      {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
),
exploded_batch_rows AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        operator,
        from_address,
        to_address,
        batch_tuple.2.1 AS token_id,
        toUInt256(batch_tuple.2.2) AS amount_raw,
        token_address,
        'CrcV2_TransferBatch' AS transfer_type
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            operator,
            from_address,
            to_address,
            token_address,
            arrayJoin(arrayZip(batch_indexes, arrayZip(token_ids, values))) AS batch_tuple
        FROM batch_rows
    )
)

SELECT * FROM single_rows
UNION ALL
SELECT * FROM exploded_batch_rows
