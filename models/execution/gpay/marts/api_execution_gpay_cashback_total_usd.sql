{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_total_usd','granularity:all_time']
  )
}}

SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUSD' AND window = 'All'
