{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_volume','granularity:last_7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'Volume' AND window = '7D'
