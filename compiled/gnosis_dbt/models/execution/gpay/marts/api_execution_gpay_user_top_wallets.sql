

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT wallet_address
FROM `dbt`.`fct_execution_gpay_user_lifetime_metrics`
WHERE total_payment_count > 0
ORDER BY
    total_payment_volume_usd DESC,
    tenure_days DESC
LIMIT 50
) AS sub