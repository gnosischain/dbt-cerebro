{{ 
  config(
   materialized='view', 
   tags=['production','execution','transactions']
   ) 
}}

SELECT
  day,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS active_accounts
FROM {{ ref('int_execution_transactions_by_project_daily') }}
GROUP BY day