{{ 
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'backing']
    )
}}

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    event_name,
    multiIf(
        event_name = 'CirclesBackingInitiated', 'initiated',
        event_name = 'CirclesBackingDeployed', 'deployed',
        event_name = 'LBPDeployed', 'lbp_deployed',
        event_name = 'CirclesBackingCompleted', 'completed',
        event_name = 'Released', 'released',
        event_name = 'AssetSupportedStatusUpdated', 'asset_status_updated',
        event_name = 'GlobalReleaseUpdated', 'global_release_updated',
        'other'
    ) AS lifecycle_stage,
    nullIf(lower(decoded_params['backer']), '') AS backer,
    nullIf(lower(decoded_params['circlesBackingInstance']), '') AS circles_backing_instance,
    nullIf(lower(decoded_params['lbp']), '') AS lbp,
    nullIf(lower(decoded_params['backingAsset']), '') AS backing_asset,
    nullIf(lower(decoded_params['personalCirclesAddress']), '') AS personal_circles_address,
    decoded_params AS event_params
FROM {{ ref('contracts_circles_v2_CirclesBackingFactory_events') }}
WHERE event_name IN (
    'AssetSupportedStatusUpdated',
    'CirclesBackingCompleted',
    'CirclesBackingDeployed',
    'CirclesBackingInitiated',
    'GlobalReleaseUpdated',
    'LBPDeployed',
    'Released'
)
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
