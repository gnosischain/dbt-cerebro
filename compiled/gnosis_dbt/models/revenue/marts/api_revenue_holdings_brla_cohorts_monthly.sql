

SELECT month, cohort, fees_total, users_cnt
FROM `dbt`.`fct_revenue_holdings_cohorts_monthly`
WHERE symbol = 'BRLA'