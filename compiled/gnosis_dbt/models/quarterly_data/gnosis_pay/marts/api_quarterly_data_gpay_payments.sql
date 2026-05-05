

SELECT
    toStartOfQuarter(month) AS quarter,
    sum(total_payment_count) AS payments
FROM `dbt`.`fct_execution_gpay_kpi_monthly`
GROUP BY quarter
ORDER BY quarter