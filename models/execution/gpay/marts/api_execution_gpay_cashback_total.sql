{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_total','granularity:all_time']
  )
}}

SELECT 'native' AS unit, value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = 'All'

UNION ALL

SELECT 'usd' AS unit, value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUSD' AND window = 'All'
