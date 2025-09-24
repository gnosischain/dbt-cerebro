{{
    config(
        materialized='view', 
        tags=['production','execution','transactions']
    )
}}

SELECT
  day,
  active_accounts AS total
FROM {{ ref('fct_execution_transactions_active_accounts_daily') }}
ORDER BY day DESC
