{{ 
  config(
    materialized='view', 
    tags=['production','execution', 'tier1', 'api:transactions_fees_per_sector', 'granularity:daily']) 
}}

SELECT
  date,
  sector AS label,
  fee_native_sum AS value
FROM {{ ref('fct_execution_transactions_by_sector_daily') }}
WHERE date < today()
ORDER BY date ASC, label ASC