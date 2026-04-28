



SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['truster']) AS truster,
    lower(decoded_params['trustee']) AS trustee,
    fromUnixTimestamp(toUInt256OrZero(decoded_params['expiryTime'])) AS expiry_time
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE event_name = 'Trust'
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_trust_updates` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_trust_updates` AS x2
        WHERE 1=1 
      )
    
  

  