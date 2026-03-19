{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:balance_cohorts_amount_per_token', 'granularity:daily'],
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
  symbol                         AS token,   
  cohort_unit,
  balance_bucket                 AS label,   
  value_native_in_bucket         AS value_native,
  value_usd_in_bucket            AS value_usd
FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY
  date,
  token,
  cohort_unit,
  label
