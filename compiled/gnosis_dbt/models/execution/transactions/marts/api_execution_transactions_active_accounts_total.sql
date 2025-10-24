
SELECT value
FROM `dbt`.`fct_execution_transactions_snapshots`
WHERE label = 'ActiveAccounts' AND window = 'All'