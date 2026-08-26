

-- Mirrors int_execution_gpay_activity_daily's own incremental pattern
-- exactly (same macro, same signature) — reuse, not a new invention.
--
-- order_by must list every GROUP BY column: this is a ReplacingMergeTree, so it
-- dedupes on the ORDER BY key, and token_address was previously grouped but not
-- ordered. Two tokens sharing a symbol would then collapse into one row on merge
-- and silently lose a day's activity. No symbol currently maps to more than one
-- address (checked 2026-08-05), so this is preventive — but a token migration
-- reusing a symbol is exactly the case that would trigger it.
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