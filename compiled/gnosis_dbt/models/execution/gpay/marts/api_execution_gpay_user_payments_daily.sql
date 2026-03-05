

SELECT
    wallet_address,
    date,
    symbol AS label,
    round(toFloat64(amount_usd), 2) AS value
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Payment'
ORDER BY date