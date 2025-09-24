{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  day,
  project,
  tx_count AS total
FROM {{ ref('int_execution_transactions_by_project_daily') }}
ORDER BY day DESC, project