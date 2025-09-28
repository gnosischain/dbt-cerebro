{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
    )
}}

SELECT
  date,
  active_accounts AS value
FROM {{ ref('fct_execution_transactions_active_accounts_daily') }}
WHERE date < today()
ORDER BY date DESC