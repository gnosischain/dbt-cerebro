{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, group_address, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index, group_address)',
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
    nullIf(lower(decoded_params['owner']), '') AS owner,
    nullIf(lower(decoded_params['mintHandler']), '') AS mint_handler,
    CAST(NULL AS Nullable(String)) AS redemption_handler,
    CAST(NULL AS Nullable(String)) AS liquidity_provider,
    nullIf(lower(decoded_params['treasury']), '') AS treasury_address,
    CAST(NULL AS Nullable(String)) AS service,
    CAST(NULL AS Nullable(String)) AS fee_collection,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_BaseGroupFactory_events') }}
WHERE event_name = 'BaseGroupCreated'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['proxy']) AS group_address,
    nullIf(lower(decoded_params['owner']), '') AS owner,
    nullIf(lower(decoded_params['mintHandler']), '') AS mint_handler,
    nullIf(lower(decoded_params['redemptionHandler']), '') AS redemption_handler,
    nullIf(lower(decoded_params['liquidityProvider']), '') AS liquidity_provider,
    CAST(NULL AS Nullable(String)) AS treasury_address,
    CAST(NULL AS Nullable(String)) AS service,
    CAST(NULL AS Nullable(String)) AS fee_collection,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_CMGroupDeployer_events') }}
WHERE event_name = 'CMGroupCreated'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(contract_address) AS group_address,
    nullIf(lower(if(event_name = 'OwnerSet', decoded_params['owner'], NULL)), '') AS owner,
    nullIf(lower(if(event_name = 'MintHandlerUpdated', decoded_params['newMintHandler'], NULL)), '') AS mint_handler,
    nullIf(lower(if(event_name = 'RedemptionHandlerUpdated', decoded_params['newRedemptionHandler'], NULL)), '') AS redemption_handler,
    CAST(NULL AS Nullable(String)) AS liquidity_provider,
    CAST(NULL AS Nullable(String)) AS treasury_address,
    nullIf(lower(if(event_name = 'ServiceUpdated', decoded_params['newService'], NULL)), '') AS service,
    nullIf(lower(if(event_name = 'FeeCollectionUpdated', decoded_params['feeCollection'], NULL)), '') AS fee_collection,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v2_BaseGroup_events') }}
WHERE event_name IN ('OwnerSet', 'ServiceUpdated', 'FeeCollectionUpdated', 'MintHandlerUpdated', 'RedemptionHandlerUpdated')
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
