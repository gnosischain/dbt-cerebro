{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gpay', 'tier0', 'api:gpay_cashback', 'granularity:7d', 'window:7d']
  )
}}

SELECT 'native' AS unit, value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = '7D'

UNION ALL

SELECT 'usd' AS unit, value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackVolume' AND window = '7D'
