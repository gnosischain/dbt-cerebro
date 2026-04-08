



SELECT
    toDate(block_timestamp) AS date,
    account,
    token_address,
    max(circles_type) AS circles_type,
    sum(delta_raw) AS delta_raw,
    max(toUInt64(toUnixTimestamp(block_timestamp))) AS last_activity_ts
FROM (
    -- Debit
    SELECT
        block_timestamp,
        from_address AS account,
        token_address,
        circles_type,
        -toInt256(amount_raw) AS delta_raw
    FROM `dbt`.`int_execution_circles_v2_transfers`
    WHERE 1 = 1
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_circles_v2_balance_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_balance_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      

    UNION ALL

    -- Credit
    SELECT
        block_timestamp,
        to_address AS account,
        token_address,
        circles_type,
        toInt256(amount_raw) AS delta_raw
    FROM `dbt`.`int_execution_circles_v2_transfers`
    WHERE 1 = 1
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_circles_v2_balance_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_balance_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      
)
GROUP BY 1, 2, 3