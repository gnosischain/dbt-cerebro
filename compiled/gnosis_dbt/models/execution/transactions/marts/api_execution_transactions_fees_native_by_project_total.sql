

SELECT t.bucket AS label, t.value
FROM `dbt`.`fct_execution_transactions_by_project_snapshots` AS t
WHERE t.label = 'FeesNative' AND t.window = 'All'
ORDER BY t.value DESC