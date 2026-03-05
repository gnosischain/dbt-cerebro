

WITH gpay_wallets AS (
    SELECT address
    FROM `dbt`.`stg_gpay__wallets`
)

SELECT
    b.date,
    b.address,
    b.symbol,
    b.balance,
    b.balance_usd
FROM `dbt`.`int_execution_tokens_balances_daily` b
WHERE b.address IN (SELECT address FROM gpay_wallets)
  AND b.date >= '2023-06-01'
  AND b.date < today()
  
  
    
    

   AND 
    toStartOfMonth(toDate(b.date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_gpay_balances_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(b.date) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_gpay_balances_daily` AS x2
      WHERE 1=1 
    )
  
