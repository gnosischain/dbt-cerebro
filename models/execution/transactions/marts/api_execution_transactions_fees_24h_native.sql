{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
    )
}}

SELECT
  toDate(now()) AS date,
  SUM(fee_native_sum) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour >= now() - INTERVAL 24 HOUR