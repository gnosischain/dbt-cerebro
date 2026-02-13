{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_total_payments','granularity:all_time']
  )
}}

SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'Payments' AND window = 'All'
