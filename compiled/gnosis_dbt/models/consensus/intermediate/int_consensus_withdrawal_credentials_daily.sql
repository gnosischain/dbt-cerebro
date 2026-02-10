


SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,withdrawal_credentials
    ,COUNT(*) AS cnt
FROM `dbt`.`stg_consensus__validators`
WHERE
    slot_timestamp < today()
    AND status LIKE 'active_%'

  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_withdrawal_credentials_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_withdrawal_credentials_daily` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1, 2