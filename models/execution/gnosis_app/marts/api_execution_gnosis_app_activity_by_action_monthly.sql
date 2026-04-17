{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','activity','tier1',
          'api:gnosis_app_activity_by_action_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "activity_kind", "column": "activity_kind", "operator": "=",
           "type": "string", "description": "Activity kind"},
          {"name": "start_month", "column": "month", "operator": ">=",
           "type": "date", "description": "Inclusive start month"},
          {"name": "end_month",   "column": "month", "operator": "<=",
           "type": "date", "description": "Inclusive end month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_gnosis_app_activity_by_action_monthly') }}
ORDER BY month, activity_kind
