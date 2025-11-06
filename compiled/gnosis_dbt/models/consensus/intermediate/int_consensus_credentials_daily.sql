




SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__validators`

  
    
      
    

   WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_consensus_credentials_daily` AS x1
    )
    AND toStartOfDay(slot_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_consensus_credentials_daily` AS x2
    )
  

GROUP BY 1, 2