



SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['user']) AS truster,
    lower(decoded_params['canSendTo']) AS trustee,
    toUInt256OrZero(decoded_params['limit']) AS trust_limit,
    toString(toUInt256OrZero(decoded_params['limit'])) AS trust_value,
    block_timestamp AS updated_at
FROM `dbt`.`contracts_circles_v1_Hub_events`
WHERE event_name = 'Trust'
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v1_trust_updates` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v1_trust_updates` AS x2
      WHERE 1=1 
    )
  

  