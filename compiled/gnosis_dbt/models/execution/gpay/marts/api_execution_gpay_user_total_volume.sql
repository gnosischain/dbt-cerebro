

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT
    wallet_address,
    round(toFloat64(sum(amount_usd)), 2) AS value
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Payment'
GROUP BY wallet_address
) AS sub