
SELECT bucket AS label, value, change_pct
FROM `dbt`.`fct_execution_transactions_by_project_snapshots` t
WHERE t.label = 'ActiveAccounts' AND window = 'All'
ORDER BY value DESC