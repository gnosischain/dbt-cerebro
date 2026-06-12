{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','circles','ecosystem','tier1',
          'api:gnosis_app_circles_ecosystem_weekly_active_users','granularity:weekly']
  )
}}

-- Whole-Circles-network weekly active reach (NOT Gnosis App growth).
-- The Gnosis-App WAU is api:gnosis_app_users (weekly) / api:gnosis_app_kpi_weekly_active_users.

SELECT
    week,
    is_blacklisted,
    cnt
FROM {{ ref('fct_execution_gnosis_app_weekly_active_users_circles_ecosystem') }}
WHERE week < toStartOfWeek(today(), 1)
ORDER BY week DESC, is_blacklisted
