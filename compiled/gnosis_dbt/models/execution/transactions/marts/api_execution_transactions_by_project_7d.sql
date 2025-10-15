
SELECT t.bucket AS label, t.value, t.change_pct
FROM `dbt`.`fct_execution_transactions_by_project_snapshots` AS t
WHERE t.label = 'Transactions' AND t.window = '7D'
ORDER BY t.value DESC