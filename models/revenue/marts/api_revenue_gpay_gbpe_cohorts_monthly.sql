{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_gpay','api:revenue_gpay_gbpe_cohorts_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "cohort",     "column": "cohort", "operator": "=",  "type": "string", "description": "Cohort bucket"},
          {"name": "start_date", "column": "month",  "operator": ">=", "type": "date",   "description": "Inclusive start month"},
          {"name": "end_date",   "column": "month",  "operator": "<=", "type": "date",   "description": "Inclusive end month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT month, cohort, fees_total, users_cnt
FROM {{ ref('fct_revenue_gpay_cohorts_monthly') }}
WHERE symbol = 'GBPe'
