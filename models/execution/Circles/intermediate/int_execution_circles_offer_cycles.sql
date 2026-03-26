{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'offers']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(contract_address) AS cycle_address,
    event_name,
    decoded_params
FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }}
WHERE event_name IN ('CycleConfiguration', 'NextOfferCreated', 'NextOfferTokensDeposited', 'OfferClaimed', 'OfferTrustSynced', 'UnclaimedTokensWithdrawn')
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
