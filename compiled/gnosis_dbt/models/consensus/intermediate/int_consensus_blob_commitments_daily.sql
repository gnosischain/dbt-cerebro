



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__blob_commitments`
WHERE
    slot_timestamp < today()
    
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_blob_commitments_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_blob_commitments_daily` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1