{{ config(materialized='view', tags=['production','execution','transactions']) }}

SELECT
  bucket AS label,   -- project
  value,
  change_pct
FROM (
  SELECT bucket, value, change_pct
  FROM {{ ref('fct_execution_transactions_by_project_snapshots') }}
  WHERE label = 'Transactions' AND window = '1D'
)
ORDER BY value DESC