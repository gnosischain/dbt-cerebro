{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_payments','granularity:last_7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'Payments' AND window = '7D'
