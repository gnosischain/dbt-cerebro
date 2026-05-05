

SELECT
    toStartOfQuarter(month) AS quarter,
    round(sum(total_payment_volume_usd), 2) AS volume_usd
FROM `dbt`.`fct_execution_gpay_kpi_monthly`
GROUP BY quarter
ORDER BY quarter