{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross','tier3','api:revenue_per_user_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": false,
        "parameters": [
          {"name": "start_month", "column": "month", "operator": ">=", "type": "date", "description": "Inclusive start month"},
          {"name": "end_month",   "column": "month", "operator": "<=", "type": "date", "description": "Inclusive end month"},
          {"name": "is_revenue_active", "column": "is_revenue_active", "operator": "=", "type": "bool", "description": "Filter to users above the $0.50 / month threshold"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_revenue_per_user_monthly') }}
