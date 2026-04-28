




WITH daily AS (
    SELECT 'holdings' AS stream_type, date, user, symbol, fees
    FROM `dbt`.`int_revenue_holdings_fees_daily`
    UNION ALL
    SELECT 'sdai'     AS stream_type, date, user, symbol, fees
    FROM `dbt`.`int_revenue_sdai_fees_daily`
    UNION ALL
    SELECT 'gpay'     AS stream_type, date, user, symbol, fees
    FROM `dbt`.`int_revenue_gpay_fees_daily`
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
      AND toDate(date) >= (
        SELECT
          
            toStartOfMonth(addDays(max(toDate(x2.month)), -1))
          

        FROM `dbt`.`int_revenue_fees_monthly_per_user` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY month, stream_type, user, symbol