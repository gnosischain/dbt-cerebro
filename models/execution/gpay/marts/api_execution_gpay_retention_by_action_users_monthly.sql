{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_retention_by_action_users_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "action",
            "column": "action",
            "operator": "=",
            "type": "string",
            "description": "Action type"
          }
        ]
      }
    }
  )
}}

SELECT
    action,
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    users                    AS value
FROM {{ ref('fct_execution_gpay_retention_by_action_monthly') }}
ORDER BY action, date, label
