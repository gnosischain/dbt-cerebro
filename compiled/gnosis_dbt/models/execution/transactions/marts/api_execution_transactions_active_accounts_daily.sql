

SELECT
  date,
  active_accounts AS value
FROM `dbt`.`fct_execution_transactions_active_accounts_daily`
WHERE date < today()
ORDER BY date ASC