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
WHERE day > now() - INTERVAL 90 DAY
ORDER BY day DESC