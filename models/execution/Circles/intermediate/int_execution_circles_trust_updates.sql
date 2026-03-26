{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, truster, trustee)',
        unique_key='(version, block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'trusts']
    )
}}

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    1 AS version,
    lower(decoded_params['user']) AS truster,
    lower(decoded_params['canSendTo']) AS trustee,
    toUInt256OrZero(decoded_params['limit']) AS trust_limit,
    CAST(NULL AS Nullable(UInt256)) AS expiry_time,
    toString(toUInt256OrZero(decoded_params['limit'])) AS trust_value,
    block_timestamp AS updated_at,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v1_Hub_events') }}
WHERE event_name = 'Trust'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    2 AS version,
    lower(decoded_params['truster']) AS truster,
    lower(decoded_params['trustee']) AS trustee,
    CAST(NULL AS Nullable(UInt256)) AS trust_limit,
    toUInt256OrZero(decoded_params['expiryTime']) AS expiry_time,
    toString(toUInt256OrZero(decoded_params['expiryTime'])) AS trust_value,
    block_timestamp AS updated_at,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE event_name = 'Trust'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
