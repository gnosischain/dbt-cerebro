

WITH gpay_owners AS (
    SELECT owner AS address
    FROM `dbt`.`int_execution_gpay_wallet_owners`
)

SELECT
    b.date,
    b.symbol,
    sum(b.balance)                          AS balance,
    round(toFloat64(sum(b.balance_usd)), 2) AS balance_usd
FROM `dbt`.`int_execution_tokens_balances_daily` b
WHERE b.address IN (SELECT address FROM gpay_owners)
  AND b.date >= '2023-06-01'
  AND b.date < today()
GROUP BY b.date, b.symbol
ORDER BY b.date, b.symbol