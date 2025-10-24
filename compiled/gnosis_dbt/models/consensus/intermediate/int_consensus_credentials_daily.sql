




SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__validators`

  
    
      
    

   WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_consensus_credentials_daily` AS t
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_consensus_credentials_daily` AS t2
    )
  

GROUP BY 1, 2