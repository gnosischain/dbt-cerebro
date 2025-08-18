


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)) AS balance
    ,SUM(effective_balance/POWER(10,9)) AS effective_balance
FROM `dbt`.`stg_consensus__validators`
WHERE 
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_balances_daily`
    )
  

GROUP BY date