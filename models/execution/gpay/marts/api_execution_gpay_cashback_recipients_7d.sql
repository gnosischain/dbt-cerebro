{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_recipients_7d','granularity:7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackUsers' AND window = '7D'
