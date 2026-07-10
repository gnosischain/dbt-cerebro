

SELECT
    toStartOfQuarter(month) AS quarter,
    max(mau) AS peak_monthly_active_users,
    max(payment_mau) AS peak_monthly_payment_users
FROM `dbt`.`fct_execution_gpay_kpi_monthly`
GROUP BY quarter
ORDER BY quarter