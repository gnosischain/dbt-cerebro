{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','tier1',
          'api:gnosis_app_active_users_incl_gpay','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "date", "operator": ">=",
           "type": "date", "description": "Inclusive start date"},
          {"name": "end_date",   "column": "date", "operator": "<=",
           "type": "date", "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

-- Gnosis App Daily Active Users (DAU), "incl. Gnosis Pay" variant — daily-grain member of the
-- active-users-incl-gpay triplet that powers the dashboard resolution toggle. Same New / Returning /
-- Reactivated / Active columns as the weekly variant; latest incomplete day (today) excluded.
SELECT
    date,
    active_users,
    new_users,
    returning_users,
    reactivated_users
FROM {{ ref('fct_execution_gnosis_app_users_daily_incl_gpay') }}
WHERE date < today()
ORDER BY date DESC
