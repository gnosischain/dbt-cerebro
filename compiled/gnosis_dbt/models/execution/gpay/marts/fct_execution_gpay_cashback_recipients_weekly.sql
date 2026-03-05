

SELECT
    toStartOfWeek(date, 1)    AS week,
    uniqExact(wallet_address) AS recipients
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Cashback'
  AND toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
GROUP BY week
ORDER BY week