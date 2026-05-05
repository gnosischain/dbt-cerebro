

SELECT
    toStartOfQuarter(month) AS quarter,
    round(sum(cashback_total_usd), 2) AS cashback_usd
FROM `dbt`.`fct_execution_gpay_kpi_monthly`
GROUP BY quarter
ORDER BY quarter