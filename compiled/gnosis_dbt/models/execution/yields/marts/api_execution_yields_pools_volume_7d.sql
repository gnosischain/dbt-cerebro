

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_pools_snapshots`
WHERE metric = 'Volume_7D'
ORDER BY value DESC