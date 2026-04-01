

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_pools_snapshots`
WHERE metric = 'Volume_7D'
ORDER BY value DESC