




SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__validators`

  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_credentials_daily`
    )
  

GROUP BY 1, 2