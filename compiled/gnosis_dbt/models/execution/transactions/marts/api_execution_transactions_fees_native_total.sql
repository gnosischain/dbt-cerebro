
SELECT value, change_pct
FROM `dbt`.`fct_execution_transactions_snapshots`
WHERE label = 'FeesNative' AND window = 'All'