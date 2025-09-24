{{ 
    config(
        materialized='view', 
        tags=['production','execution','transactions','hourly']
    ) 
}}

SELECT
  hour,
  AVG(active_accounts) OVER (
    ORDER BY hour
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  ) AS moving_average,
  active_accounts AS total
FROM {{ ref('fct_execution_transactions_active_accounts_hourly_recent') }}
ORDER BY hour DESC