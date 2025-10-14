{{
  config(materialized='view', tags=['production','execution','transactions'])
}}
SELECT
  date,
  label,
  value
FROM {{ ref('fct_execution_transactions_by_project_monthly_top5') }}
WHERE metric = 'ActiveAccounts'
ORDER BY date ASC, label ASC