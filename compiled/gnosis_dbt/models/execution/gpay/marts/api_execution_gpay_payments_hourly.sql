

SELECT
    hour          AS date,
    symbol        AS label,
    payment_count AS value
FROM `dbt`.`fct_execution_gpay_payments_hourly`
ORDER BY date, label