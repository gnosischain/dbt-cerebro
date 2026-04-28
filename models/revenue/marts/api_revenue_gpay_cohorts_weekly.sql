{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_gpay','api:revenue_gpay_cohorts_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "symbol",     "column": "symbol", "operator": "=",  "type": "string", "description": "Token symbol (EURe, GBPe, USDC.e)"},
          {"name": "cohort",     "column": "cohort", "operator": "=",  "type": "string", "description": "Cohort bucket"},
          {"name": "start_date", "column": "week",   "operator": ">=", "type": "date",   "description": "Inclusive start week"},
          {"name": "end_date",   "column": "week",   "operator": "<=", "type": "date",   "description": "Inclusive end week"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT week, symbol, cohort, annual_rolling_fees_total, users_cnt
FROM {{ ref('fct_revenue_gpay_cohorts_weekly') }}
