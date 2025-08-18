


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(amount/POWER(10,9)) AS total_amount
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__deposits`
WHERE
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_deposits_daily`
    )
  

GROUP BY 1