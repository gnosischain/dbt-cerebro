{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, group_address, token_id, batch_index)',
        unique_key='(transaction_hash, log_index, batch_index, group_address, token_id, event_name)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH single_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        0 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        toString(toUInt256OrZero(decoded_params['id'])) AS token_id,
        toInt256(toUInt256OrZero(decoded_params['value'])) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
    WHERE event_name = 'CollateralLockedSingle'
      {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
),
batch_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(decoded_params['ids'], 'Array(String)')),
                    arrayZip(
                        JSONExtract(decoded_params['ids'], 'Array(String)'),
                        JSONExtract(decoded_params['values'], 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'CollateralLockedBatch'
          {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
    )
),
batch_burns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(decoded_params['ids'], 'Array(String)')),
                    arrayZip(
                        JSONExtract(decoded_params['ids'], 'Array(String)'),
                        JSONExtract(decoded_params['values'], 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'GroupRedeemCollateralBurn'
          {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
    )
),
batch_returns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        lower(decoded_params['to']) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(decoded_params['ids'], 'Array(String)')),
                    arrayZip(
                        JSONExtract(decoded_params['ids'], 'Array(String)'),
                        JSONExtract(decoded_params['values'], 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'GroupRedeemCollateralReturn'
          {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
    )
)

SELECT * FROM single_locks
UNION ALL
SELECT * FROM batch_locks
UNION ALL
SELECT * FROM batch_burns
UNION ALL
SELECT * FROM batch_returns
