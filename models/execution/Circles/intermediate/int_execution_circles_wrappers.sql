{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'wrappers']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['avatar']) AS avatar,
    lower(decoded_params['erc20Wrapper']) AS wrapper_address,
    toUInt8(toUInt256OrZero(decoded_params['circlesType'])) AS circles_type,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_ERC20Lift_events') }}
WHERE event_name = 'ERC20WrapperDeployed'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
