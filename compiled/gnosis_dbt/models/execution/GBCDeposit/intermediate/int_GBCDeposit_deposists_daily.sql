


SELECT 
    toStartOfDay(block_timestamp) AS date
    ,decoded_params['withdrawal_credentials'] AS withdrawal_credentials
    ,SUM(reinterpretAsUInt64(unhex(substring(decoded_params['amount'], 3)))) AS amount
FROM `dbt`.`contracts_GBCDeposit_events`
WHERE
    event_name = 'DepositEvent'
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_GBCDeposit_deposists_daily`
    )
  

GROUP BY 1, 2