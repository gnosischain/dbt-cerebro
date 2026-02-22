{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_active_users_weekly','granularity:weekly']
  )
}}

SELECT
    week         AS date,
    active_users AS value
FROM {{ ref('fct_execution_gpay_activity_weekly') }}
ORDER BY date
