

SELECT
    month             AS date,
    cumulative_funded AS value
FROM `dbt`.`fct_execution_gpay_activity_monthly`
ORDER BY date