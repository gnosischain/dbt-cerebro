{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
    )
}}

SELECT
  toDate(now()) AS date,
  project,
  SUM(tx_count) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY project
ORDER BY value DESC