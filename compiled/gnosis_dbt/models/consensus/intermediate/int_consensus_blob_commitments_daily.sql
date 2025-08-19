



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__blob_commitments`
WHERE
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_blob_commitments_daily`
    )
  

GROUP BY 1