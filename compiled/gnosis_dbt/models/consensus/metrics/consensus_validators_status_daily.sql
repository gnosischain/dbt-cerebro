


SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,status
    ,COUNT(*) AS cnt
FROM `consensus`.`validators`
WHERE
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`consensus_validators_status_daily`
    )
  

GROUP BY 1, 2