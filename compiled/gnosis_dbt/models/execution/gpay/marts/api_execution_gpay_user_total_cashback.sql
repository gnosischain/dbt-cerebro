

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT
    wallet_address,
    round(toFloat64(sum(amount)), 6) AS value
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Cashback'
GROUP BY wallet_address
) AS sub