{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH base AS (
  SELECT t.bucket, toFloat64(t.value) AS value
  FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} AS t
  WHERE t.label = 'ActiveAccounts' AND t.window = 'All'
),
top AS (
  SELECT bucket, value
  FROM base
  ORDER BY value DESC
  LIMIT 20
),
others AS (
  SELECT 'Others' AS bucket, sum(value) AS value
  FROM base
  WHERE bucket NOT IN (SELECT bucket FROM top)
)
SELECT bucket AS label, value FROM top
UNION ALL
SELECT bucket AS label, value FROM others WHERE value > 0
ORDER BY value DESC