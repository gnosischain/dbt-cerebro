{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_gpay','api:revenue_gpay_gbpe_cohorts_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "cohort",     "column": "cohort", "operator": "=",  "type": "string", "description": "Cohort bucket"},
          {"name": "start_date", "column": "week",   "operator": ">=", "type": "date",   "description": "Inclusive start week"},
          {"name": "end_date",   "column": "week",   "operator": "<=", "type": "date",   "description": "Inclusive end week"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT week, cohort, annual_rolling_fees_total, users_cnt
FROM {{ ref('fct_revenue_gpay_cohorts_weekly') }}
WHERE symbol = 'GBPe'
