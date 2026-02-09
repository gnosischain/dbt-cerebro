

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_pools_snapshots`
WHERE metric = 'Fees_7D'
ORDER BY value DESC