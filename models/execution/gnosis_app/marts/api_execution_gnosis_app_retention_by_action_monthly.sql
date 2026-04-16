{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','retention','tier1',
          'api:gnosis_app_retention_by_action_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "activity_kind", "column": "activity_kind", "operator": "=",
           "type": "string", "description": "Activity kind to slice retention on"},
          {"name": "start_month", "column": "cohort_month", "operator": ">=",
           "type": "date", "description": "Inclusive start cohort month"},
          {"name": "end_month",   "column": "cohort_month", "operator": "<=",
           "type": "date", "description": "Inclusive end cohort month"}
        ],
        "sort": [{"column": "cohort_month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    activity_kind,
    retention_pct,
    users                    AS value_abs,
    initial_users
FROM {{ ref('fct_execution_gnosis_app_retention_by_action_monthly') }}
ORDER BY activity_kind, y, x
