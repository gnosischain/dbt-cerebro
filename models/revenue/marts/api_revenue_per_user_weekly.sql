{{
  config(
    materialized='view',
    tags=['production', 'revenue', 'revenue_cross', 'tier3', 'api:revenue_per_user', 'granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": false,
        "parameters": [
          {"name": "start_week", "column": "week", "operator": ">=", "type": "date", "description": "Inclusive start week"},
          {"name": "end_week",   "column": "week", "operator": "<=", "type": "date", "description": "Inclusive end week"},
          {"name": "is_revenue_active", "column": "is_revenue_active", "operator": "=", "type": "bool", "description": "Filter to users above the $6 / 52w threshold"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_revenue_per_user_weekly') }}
