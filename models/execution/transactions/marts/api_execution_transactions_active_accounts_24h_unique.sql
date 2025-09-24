{{ 
    config(
        materialized='view', 
        tags=['production','execution','transactions','hourly']
    ) 
}}

SELECT
  bitmapCardinality(groupBitmapMerge(ua_bitmap_state)) AS value
FROM {{ ref('fct_execution_transactions_active_accounts_hourly_recent') }}
WHERE hour > now() - INTERVAL 1 DAY