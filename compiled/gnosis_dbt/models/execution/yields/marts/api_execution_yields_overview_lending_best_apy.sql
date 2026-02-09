

SELECT
    value,
    change_pct,
    label
FROM `dbt`.`fct_execution_yields_overview_snapshot`
WHERE metric = 'lending_best_apy'