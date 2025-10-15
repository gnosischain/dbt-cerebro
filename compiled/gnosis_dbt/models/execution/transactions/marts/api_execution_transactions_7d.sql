
SELECT value, change_pct
FROM `dbt`.`fct_execution_transactions_snapshots`
WHERE label = 'Transactions' AND window = '7D'