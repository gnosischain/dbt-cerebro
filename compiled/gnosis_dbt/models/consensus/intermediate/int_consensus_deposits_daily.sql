


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(amount/POWER(10,9)) AS total_amount
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__deposits`
WHERE
    slot_timestamp < today()
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_consensus_deposits_daily` AS t
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_consensus_deposits_daily` AS t2
    )
  

GROUP BY 1