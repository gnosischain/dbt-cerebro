{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_7d','granularity:7d']
  )
}}

SELECT 'native' AS unit, value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = '7D'

UNION ALL

SELECT 'usd' AS unit, value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUSD' AND window = '7D'
