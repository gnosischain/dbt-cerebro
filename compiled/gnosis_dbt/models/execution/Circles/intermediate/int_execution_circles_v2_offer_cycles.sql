



SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(contract_address) AS cycle_address,
    event_name,
    decoded_params
FROM `dbt`.`contracts_circles_v2_ERC20TokenOfferCycle_events`
WHERE event_name IN ('CycleConfiguration', 'NextOfferCreated', 'NextOfferTokensDeposited', 'OfferClaimed', 'OfferTrustSynced', 'UnclaimedTokensWithdrawn')
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_offer_cycles` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_offer_cycles` AS x2
        WHERE 1=1 
      )
    
  

  