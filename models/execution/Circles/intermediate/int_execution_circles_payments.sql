{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'payments']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['payer']) AS payer,
    lower(decoded_params['payee']) AS payee,
    lower(decoded_params['gateway']) AS gateway,
    toString(toUInt256OrZero(decoded_params['tokenId'])) AS token_id,
    toUInt256OrZero(decoded_params['amount']) AS amount_raw,
    decoded_params['data'] AS payment_data,
    decoded_params AS event_params
FROM {{ ref('contracts_circles_v2_PaymentGatewayFactory_events') }}
WHERE event_name = 'PaymentReceived'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
