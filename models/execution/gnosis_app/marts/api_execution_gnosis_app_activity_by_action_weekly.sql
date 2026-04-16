{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','activity','tier1',
          'api:gnosis_app_activity_by_action_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "activity_kind", "column": "activity_kind", "operator": "=",
           "type": "string", "description": "Activity kind"},
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

SELECT * FROM {{ ref('fct_execution_gnosis_app_activity_by_action_weekly') }}
ORDER BY week, activity_kind
