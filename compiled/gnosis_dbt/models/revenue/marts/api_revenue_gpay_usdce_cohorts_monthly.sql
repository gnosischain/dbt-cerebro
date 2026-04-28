

SELECT month, cohort, fees_total, users_cnt
FROM `dbt`.`fct_revenue_gpay_cohorts_monthly`
WHERE symbol = 'USDC.e'