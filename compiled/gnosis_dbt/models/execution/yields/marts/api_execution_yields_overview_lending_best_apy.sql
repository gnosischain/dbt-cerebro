

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_pools_daily`) AS as_of_date
FROM (
SELECT
    value,
    change_pct,
    label
FROM `dbt`.`fct_execution_yields_overview_snapshot`
WHERE metric = 'lending_best_apy'
) AS sub