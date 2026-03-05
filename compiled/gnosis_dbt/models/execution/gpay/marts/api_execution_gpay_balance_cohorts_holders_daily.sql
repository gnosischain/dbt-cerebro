

SELECT
    date,
    symbol         AS token,
    cohort_unit,
    balance_bucket AS label,
    holders        AS value
FROM `dbt`.`fct_execution_gpay_balance_cohorts_daily`
ORDER BY date, token, cohort_unit, label