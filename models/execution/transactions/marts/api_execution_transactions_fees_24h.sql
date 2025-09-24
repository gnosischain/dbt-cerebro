{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','hourly']
)
}}

SELECT
  SUM(fee_native_sum) AS total_fee_native,
  SUM(fee_usd_sum)    AS total_fee_usd
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
WHERE hour > now() - INTERVAL 1 DAY