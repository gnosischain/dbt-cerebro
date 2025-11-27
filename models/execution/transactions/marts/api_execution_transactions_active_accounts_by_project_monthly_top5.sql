{{
  config(materialized='view', tags=['production','execution','transactions', 'tier1', 'api: initiator_accounts_by_project_m_top5'])
}}
SELECT
  date,
  label,
  value
FROM {{ ref('fct_execution_transactions_by_project_monthly_top5') }}
WHERE metric = 'ActiveAccounts'
ORDER BY date ASC, label ASC