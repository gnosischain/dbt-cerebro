{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gpay', 'tier0', 'api:gpay_cashback_recipients', 'granularity:7d', 'window:7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUsers' AND window = '7D'
