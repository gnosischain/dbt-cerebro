




-- Thin UNION ALL over the monthly-partitioned in/out int_ legs. The heavy
-- group-by + counterparty cardinality blow-up lives upstream so refreshing
-- this fct is cheap.

WITH all_legs AS (
  SELECT
    date,
    token_address,
    symbol,
    address,
    counterparty,
    direction,
    net_amount_raw,
    gross_amount_raw,
    transfer_count
  FROM `dbt`.`int_execution_account_token_movements_out_daily`

  UNION ALL

  SELECT
    date,
    token_address,
    symbol,
    address,
    counterparty,
    direction,
    net_amount_raw,
    gross_amount_raw,
    transfer_count
  FROM `dbt`.`int_execution_account_token_movements_in_daily`
)

SELECT
  date,
  token_address,
  symbol,
  'WHITELISTED' AS token_class,
  address,
  counterparty,
  direction,
  net_amount_raw,
  gross_amount_raw,
  transfer_count
FROM all_legs
WHERE address != '0x0000000000000000000000000000000000000000'
  AND counterparty IS NOT NULL
  AND counterparty != ''
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_execution_account_token_movements_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`fct_execution_account_token_movements_daily` AS x2
        WHERE 1=1 
      )
    
  

  