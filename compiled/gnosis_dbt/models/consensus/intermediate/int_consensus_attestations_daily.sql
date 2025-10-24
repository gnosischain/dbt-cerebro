



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,slot - attestation_slot AS inclusion_delay
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__attestations`
WHERE
    slot_timestamp < today()
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_consensus_attestations_daily` AS t
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_consensus_attestations_daily` AS t2
    )
  

GROUP BY 1, 2