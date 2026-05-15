

SELECT
    date,
    cohort,
    cohort_order,
    cnt
FROM `dbt`.`fct_execution_circles_v2_minter_cohort_daily`
WHERE date < today()
ORDER BY date DESC, cohort_order