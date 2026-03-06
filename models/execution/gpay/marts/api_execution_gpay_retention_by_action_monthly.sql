{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_retention_by_action_monthly','granularity:monthly'],
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
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    action,
    retention_pct,
    users                    AS value_abs,
    amount_retention_pct,
    amount_usd               AS value_usd
FROM {{ ref('fct_execution_gpay_retention_by_action_monthly') }}
ORDER BY action, y, x
