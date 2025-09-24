{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  month,
  active_accounts AS total
FROM {{ ref('fct_execution_transactions_active_accounts_monthly') }}
ORDER BY month DESC