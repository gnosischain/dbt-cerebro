






-- Per-token daily Circles v2 supply, derived from the zero-address balance
-- in int_execution_circles_v2_balances_daily. Built as `int_` so it can run
-- incrementally; the `fct_` mart is a thin view over this table.

WITH balances AS (
    SELECT *
    FROM `dbt`.`int_execution_circles_v2_balances_daily`
    WHERE account = '0x0000000000000000000000000000000000000000'
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_execution_circles_v2_tokens_supply_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_execution_circles_v2_tokens_supply_daily` AS x2
        WHERE 1=1 
      )
    
  

    
)
SELECT
    date,
    token_address,
    -balance_raw AS supply_raw,
    -balance_raw / POWER(10, 18) AS supply,
    -demurraged_balance_raw / POWER(10, 18) AS demurraged_supply
FROM balances