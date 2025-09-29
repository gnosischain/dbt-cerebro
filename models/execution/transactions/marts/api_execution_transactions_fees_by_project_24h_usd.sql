{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
  )
}}

SELECT
  toDate(now()) AS date,
  project AS label,
  SUM(fee_usd_sum) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY label
ORDER BY value DESC