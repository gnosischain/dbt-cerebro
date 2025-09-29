{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  date,
  project AS label,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS value
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE date < today()
GROUP BY date, label
ORDER BY date DESC, label