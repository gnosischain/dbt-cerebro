{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  day,
  project,
  active_accounts AS total
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE day > now() - INTERVAL 90 DAY
ORDER BY day DESC, project