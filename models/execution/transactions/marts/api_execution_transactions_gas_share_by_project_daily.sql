{{
  config(materialized='view', tags=['production','execution','transactions','gas'])
}}

WITH proj AS (
  SELECT
    day,
    project,
    gas_used_sum AS project_gas_used
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),
tot AS (
  SELECT
    day,
    SUM(gas_used_sum) AS day_gas_used
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  GROUP BY day
)
SELECT
  p.day,
  p.project,
  p.project_gas_used / NULLIF(t.day_gas_used, 0) AS share_of_used  
FROM proj p
JOIN tot t USING (day)
ORDER BY p.day DESC, p.project