

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_pools_daily`) AS as_of_date
FROM (
SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_pools_snapshots`
WHERE metric = 'Volume_7D'
ORDER BY value DESC
) AS sub