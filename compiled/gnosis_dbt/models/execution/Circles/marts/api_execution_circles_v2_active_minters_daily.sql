

SELECT
    date,
    active_minters
FROM `dbt`.`fct_execution_circles_v2_active_minters_daily`
WHERE date < today()
ORDER BY date DESC