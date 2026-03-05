

SELECT
    address AS wallet_address,
    date,
    symbol AS label,
    symbol AS token,
    round(toFloat64(balance), 6)     AS value_native,
    round(toFloat64(balance_usd), 2) AS value_usd
FROM `dbt`.`int_execution_gpay_balances_daily`
ORDER BY date