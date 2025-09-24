{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  date_trunc('month', day) AS month,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS total
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE day > now() - INTERVAL 3 MONTH
GROUP BY month
ORDER BY month DESC