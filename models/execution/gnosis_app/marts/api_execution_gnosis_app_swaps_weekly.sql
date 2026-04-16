{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','cow','swaps','tier1',
          'api:gnosis_app_swaps_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "week", "operator": ">=",
           "type": "date", "description": "Inclusive start week"},
          {"name": "end_date",   "column": "week", "operator": "<=",
           "type": "date", "description": "Inclusive end week"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_gnosis_app_swaps_weekly') }} ORDER BY week
