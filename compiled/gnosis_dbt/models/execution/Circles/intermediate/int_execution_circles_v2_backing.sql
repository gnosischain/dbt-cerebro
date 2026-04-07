



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
FROM `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`
WHERE event_name IN (
    'AssetSupportedStatusUpdated',
    'CirclesBackingCompleted',
    'CirclesBackingDeployed',
    'CirclesBackingInitiated',
    'GlobalReleaseUpdated',
    'LBPDeployed',
    'Released'
)
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_backing` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_backing` AS x2
      WHERE 1=1 
    )
  

  