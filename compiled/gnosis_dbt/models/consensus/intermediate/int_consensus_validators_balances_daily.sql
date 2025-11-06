


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)) AS balance
    ,SUM(effective_balance/POWER(10,9)) AS effective_balance
FROM `dbt`.`stg_consensus__validators`
WHERE 
    slot_timestamp < today()
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_consensus_validators_balances_daily` AS x1
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_consensus_validators_balances_daily` AS x2
    )
  

GROUP BY date