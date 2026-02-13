{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_active_users','granularity:last_7d']
  )
}}

SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'ActiveUsers' AND window = '7D'
