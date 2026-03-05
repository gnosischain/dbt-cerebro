

SELECT
    wallet_address,
    sum(activity_count) AS value
FROM `dbt`.`int_execution_gpay_activity_daily`
WHERE action = 'Payment'
GROUP BY wallet_address