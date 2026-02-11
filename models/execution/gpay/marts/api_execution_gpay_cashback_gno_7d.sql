{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_gno_7d','granularity:7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = '7D'
