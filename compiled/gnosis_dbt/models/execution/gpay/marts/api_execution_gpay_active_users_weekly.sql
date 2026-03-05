

SELECT
    week         AS date,
    active_users AS value
FROM `dbt`.`fct_execution_gpay_activity_weekly`
ORDER BY date