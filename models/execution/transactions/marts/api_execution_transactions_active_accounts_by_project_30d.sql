{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  project,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS total
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE day > now() - INTERVAL 30 DAY
GROUP BY project
ORDER BY total DESC