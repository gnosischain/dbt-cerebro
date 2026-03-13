

WITH gpay_wallets AS (
    SELECT address, introduced_at
    FROM `dbt`.`stg_gpay__wallets`
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
  AND b.date >= w.introduced_at
  AND b.date < today()