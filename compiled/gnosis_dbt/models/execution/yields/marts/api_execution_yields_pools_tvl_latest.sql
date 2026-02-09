


SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_pools_snapshots`
WHERE metric = 'TVL_Latest'
ORDER BY value DESC