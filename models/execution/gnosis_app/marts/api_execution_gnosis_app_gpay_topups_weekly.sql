{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','gpay','topups','tier1',
          'api:gnosis_app_gpay_topups_weekly','granularity:weekly'],
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

SELECT * FROM {{ ref('fct_execution_gnosis_app_gpay_topups_weekly') }} ORDER BY week
