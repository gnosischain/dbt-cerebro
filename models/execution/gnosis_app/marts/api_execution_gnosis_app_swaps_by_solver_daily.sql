{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','cow','swaps','tier1',
          'api:gnosis_app_swaps_by_solver_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "solver", "column": "solver", "operator": "=",
           "type": "string", "description": "Solver address"},
          {"name": "start_date", "column": "date", "operator": ">=",
           "type": "date", "description": "Inclusive start date"},
          {"name": "end_date",   "column": "date", "operator": "<=",
           "type": "date", "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_gnosis_app_swaps_by_solver_daily') }}
ORDER BY date, solver
