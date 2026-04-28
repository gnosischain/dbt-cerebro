




-- Thin pass-through over int_execution_account_balance_history_daily.
-- The heavy address × token × date aggregation lives in the int_ model so
-- this layer is cheap to refresh.

SELECT
  address,
  date,
  total_balance_usd,
  tokens_held,
  native_or_wrapped_xdai_balance,
  priced_balance_usd,
  priced_tokens_held
FROM `dbt`.`int_execution_account_balance_history_daily`
WHERE date < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_execution_account_balance_history_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`fct_execution_account_balance_history_daily` AS x2
        WHERE 1=1 
      )
    
  

  