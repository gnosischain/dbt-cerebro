

SELECT
    wallet_address,
    date,
    round(toFloat64(amount), 6) AS value
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Cashback'
ORDER BY date