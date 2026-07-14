{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','tier1',
          'api:gnosis_app_weekly_active_users_incl_gpay','granularity:weekly']
  )
}}

-- Gnosis App Weekly Active Users (WAU) — "incl. Gnosis Pay" variant. Same columns as
-- api_execution_gnosis_app_weekly_active_users, but the population additionally counts any
-- user-initiated Gnosis Pay card-wallet transaction (attributed to the safe's GA owner).
-- Shown side-by-side with the current WAU for comparison. Latest incomplete week excluded.
SELECT
    week,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM {{ ref('fct_execution_gnosis_app_users_weekly_incl_gpay') }}
WHERE week < toStartOfWeek(today(), 1)
ORDER BY week DESC
