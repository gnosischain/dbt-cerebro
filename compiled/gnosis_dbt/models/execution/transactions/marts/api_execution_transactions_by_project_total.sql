

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_transactions_by_project_daily`) AS as_of_date
FROM (
SELECT t.bucket AS label, t.value
FROM `dbt`.`fct_execution_transactions_by_project_snapshots` AS t
WHERE t.label = 'Transactions' AND t.window = 'All'
ORDER BY t.value DESC
) AS sub