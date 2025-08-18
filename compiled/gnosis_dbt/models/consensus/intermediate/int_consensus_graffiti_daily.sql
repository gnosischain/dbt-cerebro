



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,graffiti
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__blocks`
WHERE
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_graffiti_daily`
    )
  

GROUP BY 1, 2