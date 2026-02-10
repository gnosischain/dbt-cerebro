




SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__validators`

  
    
    

   WHERE 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_credentials_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_credentials_daily` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1, 2