{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_churn_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "scope",
            "column": "scope",
            "operator": "=",
            "type": "string",
            "description": "Churn scope"
          },
          {
            "name": "start_date",
            "column": "month",
            "operator": ">=",
            "type": "date",
            "description": "Inclusive start month"
          },
          {
            "name": "end_date",
            "column": "month",
            "operator": "<=",
            "type": "date",
            "description": "Inclusive end month"
          }
        ],
        "sort": [
          {"column": "month", "direction": "DESC"}
        ]
      }
    }
  )
}}

SELECT
    scope,
    toString(month) AS month,
    new_users,
    retained_users,
    returning_users,
    churned_users,
    total_active,
    churn_rate,
    retention_rate
FROM {{ ref('fct_execution_gpay_churn_monthly') }}
ORDER BY scope, month
