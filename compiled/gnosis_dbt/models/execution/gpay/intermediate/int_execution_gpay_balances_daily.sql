

WITH gpay_wallets AS (
    SELECT address, activation_date
    FROM `dbt`.`int_execution_gpay_wallets`
)

SELECT
    b.date,
    b.address,
    b.symbol,
    b.balance,
    b.balance_usd
FROM `dbt`.`int_execution_tokens_balances_daily` b
INNER JOIN gpay_wallets w 
  ON b.address = w.address
WHERE 
  b.date >= '2023-06-01'
  AND b.date >= w.activation_date
  AND b.date < today()