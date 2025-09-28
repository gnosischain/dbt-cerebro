{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
    )
}}

SELECT
  date,
  project,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS value
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE date < today()
GROUP BY date, project
ORDER BY date DESC, project