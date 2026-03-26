{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['group']) AS group_address,
    lower(decoded_params['mint']) AS mint_policy,
    lower(decoded_params['treasury']) AS treasury_address,
    decoded_params['name'] AS group_name,
    decoded_params['symbol'] AS group_symbol,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE event_name = 'RegisterGroup'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
