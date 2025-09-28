{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
    )
}}

SELECT
  month AS date,
  active_accounts AS value
FROM {{ ref('fct_execution_transactions_active_accounts_monthly') }}
WHERE month < date_trunc('month', today())
ORDER BY date DESC