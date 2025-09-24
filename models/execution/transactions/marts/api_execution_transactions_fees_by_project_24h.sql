{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
  )
}}

SELECT
  project,
  SUM(fee_native_sum) AS fee_native,
  SUM(fee_usd_sum)    AS fee_usd
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour > now() - INTERVAL 1 DAY
GROUP BY project
ORDER BY fee_usd DESC