{{
  config(materialized='view', tags=['production','execution','transactions','hourly', 'tier1', 'api: cnt_by_sector_h'])
}}

SELECT
  hour AS date,
  sector AS label,
  sum(tx_count) AS value
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
GROUP BY date, label
ORDER BY date ASC, label ASC