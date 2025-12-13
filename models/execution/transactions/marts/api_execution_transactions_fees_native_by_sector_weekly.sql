{{ 
  config(
    materialized='view', 
    tags=['production','execution', 'tier1', 'api:transactions_fees_per_sector', 'granularity:weekly']) 
}}

SELECT
  week AS date,
  sector AS label,
  fee_native_sum AS value
FROM {{ ref('fct_execution_transactions_by_sector_weekly') }}
ORDER BY date ASC, label ASC