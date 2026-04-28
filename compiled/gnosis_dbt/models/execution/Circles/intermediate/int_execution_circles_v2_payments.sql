



SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['payer']) AS payer,
    lower(decoded_params['payee']) AS payee,
    lower(decoded_params['gateway']) AS gateway,
    toString(toUInt256OrZero(decoded_params['tokenId'])) AS token_id,
    toUInt256OrZero(decoded_params['amount']) AS amount_raw,
    decoded_params['data'] AS payment_data,
    decoded_params AS event_params
FROM `dbt`.`contracts_circles_v2_PaymentGatewayFactory_events`
WHERE event_name = 'PaymentReceived'
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_payments` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_payments` AS x2
        WHERE 1=1 
      )
    
  

  