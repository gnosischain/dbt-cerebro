
SELECT
  date,
  label,
  value
FROM `dbt`.`fct_execution_transactions_by_project_monthly_top5`
WHERE metric = 'ActiveAccounts'
ORDER BY date ASC, label ASC