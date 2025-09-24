{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
  )
}}

SELECT
  project,
  SUM(tx_count) AS total
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour > now() - INTERVAL 1 DAY
GROUP BY project
ORDER BY total DESC