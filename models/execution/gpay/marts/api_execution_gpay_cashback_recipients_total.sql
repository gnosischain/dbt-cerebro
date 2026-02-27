{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_recipients_total','granularity:all_time']
  )
}}

SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUsers' AND window = 'All'
