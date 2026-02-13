{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_total_balance','granularity:all_time']
  )
}}

SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'TotalBalance' AND window = 'All'
