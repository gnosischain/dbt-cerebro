{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
    )
}}

SELECT
  hour AS date,
  AVG(active_accounts) OVER (
    ORDER BY hour
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  ) AS value
FROM {{ ref('fct_execution_transactions_active_accounts_hourly_recent') }}
ORDER BY date DESC