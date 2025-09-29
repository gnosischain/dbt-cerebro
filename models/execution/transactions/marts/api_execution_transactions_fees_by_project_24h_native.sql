{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
  )
}}

SELECT
  toDate(now()) AS date,
  project AS label,
  ROUND(SUM(fee_native_sum), 6) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY label
ORDER BY value DESC