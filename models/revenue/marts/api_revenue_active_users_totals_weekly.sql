{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross','api:revenue_active_users_totals_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "week", "operator": ">=", "type": "date", "description": "Inclusive start week"},
          {"name": "end_date",   "column": "week", "operator": "<=", "type": "date", "description": "Inclusive end week"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT week, users_cnt, annual_rolling_fees_total
FROM {{ ref('fct_revenue_active_users_totals_weekly') }}
