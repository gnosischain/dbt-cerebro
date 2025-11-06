



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__blob_commitments`
WHERE
    slot_timestamp < today()
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_consensus_blob_commitments_daily` AS x1
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_consensus_blob_commitments_daily` AS x2
    )
  

GROUP BY 1