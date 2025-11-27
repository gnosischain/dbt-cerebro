{{
  config(materialized='view', tags=['production','execution','transactions','hourly', 'tier1', 'api: fees_by_sector_h'])
}}

SELECT
  hour AS date,
  sector AS label,
  round(toFloat64(sum(fee_native_sum)), 2) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
GROUP BY date, label
ORDER BY date ASC, label ASC