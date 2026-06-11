{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','tier1',
          'api:gnosis_app_weekly_active_users_in_app','granularity:weekly']
  )
}}

SELECT
    week,
    is_blacklisted,
    cnt
FROM {{ ref('fct_execution_gnosis_app_weekly_active_users_in_app') }}
ORDER BY week DESC, is_blacklisted
