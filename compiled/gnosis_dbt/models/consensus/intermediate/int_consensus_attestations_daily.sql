



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,slot - attestation_slot AS inclusion_delay
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__attestations`
WHERE
    slot_timestamp < today()
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_consensus_attestations_daily` AS x1
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_consensus_attestations_daily` AS x2
    )
  

GROUP BY 1, 2