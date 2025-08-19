



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,slot - attestation_slot AS inclusion_delay
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__attestations`
WHERE
    slot_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_attestations_daily`
    )
  

GROUP BY 1, 2