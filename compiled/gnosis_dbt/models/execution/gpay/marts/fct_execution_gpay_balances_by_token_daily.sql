

SELECT
    date,
    symbol,
    sum(balance)                          AS balance,
    round(toFloat64(sum(balance_usd)), 2) AS balance_usd
FROM `dbt`.`int_execution_gpay_balances_daily`
GROUP BY date, symbol
ORDER BY date, symbol