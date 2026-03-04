{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_balance_cohorts_holders_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "token",
            "column": "token",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
          },
          {
            "name": "start_date",
            "column": "date",
            "operator": ">=",
            "type": "date",
            "description": "Inclusive start date"
          },
          {
            "name": "end_date",
            "column": "date",
            "operator": "<=",
            "type": "date",
            "description": "Inclusive end date"
          }
        ],
        "sort": [
          {"column": "date", "direction": "DESC"}
        ]
      }
    }
  )
}}

SELECT
    date,
    symbol         AS token,
    cohort_unit,
    balance_bucket AS label,
    holders        AS value
FROM {{ ref('fct_execution_gpay_balance_cohorts_daily') }}
ORDER BY date, token, cohort_unit, label
