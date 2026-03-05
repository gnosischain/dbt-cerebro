

SELECT
    date,
    symbol         AS token,
    cohort_unit,
    balance_bucket AS label,
    value_native,
    value_usd
FROM `dbt`.`fct_execution_gpay_balance_cohorts_daily`
ORDER BY date, token, cohort_unit, label