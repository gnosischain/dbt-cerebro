

-- Mirrors int_execution_gpay_activity_daily's own incremental pattern
-- exactly (same macro, same signature) — reuse, not a new invention.
SELECT
    date,
    safe_address,
    action,
    token_symbol,
    token_address,
    SUM(amount)                        AS amount,
    SUM(amount_usd)                    AS amount_usd,
    COUNT()                            AS activity_count
FROM `dbt`.`int_celo_gpay_activity`

  
    
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_celo_gpay_activity_daily` AS x1
        WHERE 1=1 
      )
      
    
  

GROUP BY date, safe_address, action, token_symbol, token_address