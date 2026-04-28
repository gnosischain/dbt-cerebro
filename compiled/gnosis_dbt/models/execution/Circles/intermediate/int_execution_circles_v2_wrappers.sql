



SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['avatar']) AS avatar,
    lower(decoded_params['erc20Wrapper']) AS wrapper_address,
    toUInt8(toUInt256OrZero(decoded_params['circlesType'])) AS circles_type
FROM `dbt`.`contracts_circles_v2_ERC20Lift_events`
WHERE event_name = 'ERC20WrapperDeployed'
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_wrappers` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_wrappers` AS x2
        WHERE 1=1 
      )
    
  

  