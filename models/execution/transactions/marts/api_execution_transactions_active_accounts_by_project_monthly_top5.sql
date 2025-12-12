{{
  config(
    materialized='view', 
    tags=['production','execution', 'tier1', 'api:transactions_initiators_count_per_project_top5', 'granularity:monthly'])
}}
SELECT
  date,
  label,
  value
FROM {{ ref('fct_execution_transactions_by_project_monthly_top5') }}
WHERE metric = 'ActiveAccounts'
ORDER BY date ASC, label ASC