{{
  config(
    materialized='view',
    tags=['production','execution','transactions']
  )
}}

SELECT
  project,
  SUM(tx_count) AS total
FROM {{ ref('int_execution_transactions__by_project_daily') }}
WHERE day > now() - INTERVAL 7 DAY
GROUP BY project
ORDER BY total DESC