{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_total_gno','granularity:all_time']
  )
}}

SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = 'All'
