{{ 
  config(
   materialized='view', 
   tags=['production','execution','transactions','hourly']
   ) 
}}

SELECT
  hour,
  groupBitmapMerge(ua_bitmap_state)                     AS ua_bitmap_state,
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state))  AS active_accounts
FROM {{ ref('int_execution_transactions_by_project_hourly_recent') }}
GROUP BY hour