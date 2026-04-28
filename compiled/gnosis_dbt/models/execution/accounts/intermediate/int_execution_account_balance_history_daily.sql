




-- Heavy address × token × date aggregate, checkpointed into its own
-- monthly-partitioned int_ table so the parent fct_ stays a thin pass-through
-- and full-refresh runs in monthly chunks instead of materializing the entire
-- aggregating transform in RAM.

WITH balances AS (
  SELECT
    lower(address) AS address,
    date,
    symbol,
    balance,
    ifNull(balance_usd, 0) AS balance_usd
  FROM `dbt`.`int_execution_tokens_balances_daily`
  WHERE address IS NOT NULL
    AND address != ''
    AND date < today()
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_execution_account_balance_history_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_execution_account_balance_history_daily` AS x2
        WHERE 1=1 
      )
    
  

    
)

SELECT
  address,
  date,
  sum(balance_usd) AS total_balance_usd,
  countIf(balance > 0) AS tokens_held,
  maxIf(balance, upper(symbol) IN ('XDAI', 'WXDAI')) AS native_or_wrapped_xdai_balance,
  sumIf(balance_usd, balance_usd > 0) AS priced_balance_usd,
  countIf(balance > 0 AND balance_usd > 0) AS priced_tokens_held
FROM balances
GROUP BY address, date