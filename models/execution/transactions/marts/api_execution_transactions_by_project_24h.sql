{{
  config(
    materialized='view',
    tags=['production','execution','transactions','hourly']
  )
}}

SELECT
  project,
  SUM(tx_count) AS total
FROM {{ ref('fct_execution_transactions__by_project_hourly_recent') }}
WHERE hour > now() - INTERVAL 1 DAY
GROUP BY project
ORDER BY total DESC