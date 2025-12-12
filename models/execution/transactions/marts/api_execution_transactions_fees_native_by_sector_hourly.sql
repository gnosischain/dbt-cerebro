{{
  config(
    materialized='view', 
    tags=['production','execution', 'tier1', 'api:transactions_fees_per_sector', 'granularity:hourly'])
}}

SELECT
  hour AS date,
  sector AS label,
  round(toFloat64(sum(fee_native_sum)), 2) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
GROUP BY date, label
ORDER BY date ASC, label ASC