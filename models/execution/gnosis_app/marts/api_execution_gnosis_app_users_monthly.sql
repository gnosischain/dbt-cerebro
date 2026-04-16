{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','users','tier1',
          'api:gnosis_app_users_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "month", "operator": ">=",
           "type": "date", "description": "Inclusive start month"},
          {"name": "end_date",   "column": "month", "operator": "<=",
           "type": "date", "description": "Inclusive end month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_gnosis_app_users_monthly') }} ORDER BY month
