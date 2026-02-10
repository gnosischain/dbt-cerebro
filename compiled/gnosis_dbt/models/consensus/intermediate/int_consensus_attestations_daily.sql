



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,slot - attestation_slot AS inclusion_delay
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__attestations`
WHERE
    slot_timestamp < today()
    
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_attestations_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_attestations_daily` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1, 2