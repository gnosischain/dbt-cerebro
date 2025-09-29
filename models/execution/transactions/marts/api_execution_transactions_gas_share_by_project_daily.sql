{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','gas']
  )
}}

WITH tot AS (
  SELECT
    date,
    SUM(gas_used_sum) AS day_gas_used
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  WHERE date < today()
  GROUP BY date
)
SELECT
  p.date,
  p.project AS label,
  p.gas_used_sum / NULLIF(t.day_gas_used, 0) AS value
FROM {{ ref('int_execution_transactions_by_project_daily') }} p
JOIN tot t USING (date)
WHERE p.date < today()
ORDER BY p.date DESC, label