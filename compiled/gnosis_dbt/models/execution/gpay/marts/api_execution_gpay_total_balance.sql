

SELECT value
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'TotalBalance' AND window = 'All'