


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)) AS balance
    ,SUM(effective_balance/POWER(10,9)) AS effective_balance
FROM `dbt`.`stg_consensus__validators`
WHERE 
    slot_timestamp < today()
    
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_validators_balances_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_validators_balances_daily` AS x2
      WHERE 1=1 
    )
  

GROUP BY date