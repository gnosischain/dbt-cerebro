





-- Reads the unified view (single canonicalization junction for the June
-- 2026 Safe migration) instead of re-unioning the stream models, so the
-- per-user key here always matches the cross-stream canonical address.
WITH daily AS (
    SELECT stream_type, date, user, symbol, fees
    FROM `dbt`.`int_revenue_fees_unified_daily`
)

SELECT
    toStartOfMonth(date) AS month,
    stream_type,
    user,
    symbol,
    round(sum(fees), 8) AS month_fees
FROM daily
WHERE toStartOfMonth(date) < toStartOfMonth(today())
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.month)), -1))
        FROM `dbt`.`int_revenue_fees_monthly_per_user` AS x1
        WHERE 1=1 
      )
    
  

  
GROUP BY month, stream_type, user, symbol