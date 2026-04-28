

SELECT week, cohort, annual_rolling_fees_total, users_cnt
FROM `dbt`.`fct_revenue_gpay_cohorts_weekly`
WHERE symbol = 'EURe'