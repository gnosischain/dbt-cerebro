








SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,SUM(amount) / POWER(10, 9) AS withdrawals_amount_gno
    ,COUNT(*) AS withdrawals_count
FROM `dbt`.`stg_consensus__withdrawals`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_withdrawals_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(slot_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_consensus_validators_withdrawals_daily` AS x2
        WHERE 1=1 
  

      )
    
  

    
    
GROUP BY 1, 2