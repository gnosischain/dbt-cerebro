

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_transactions_by_project_daily`) AS as_of_date
FROM (
SELECT value
FROM `dbt`.`fct_execution_transactions_snapshots`
WHERE label = 'ActiveAccounts' AND window = 'All'
) AS sub