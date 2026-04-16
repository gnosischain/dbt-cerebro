{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','activity','tier1',
          'api:gnosis_app_activity_by_action_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "activity_kind", "column": "activity_kind", "operator": "=",
           "type": "string", "description": "Activity kind (onboard, swap_filled, topup, marketplace_buy, etc.)"},
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

SELECT * FROM {{ ref('fct_execution_gnosis_app_activity_by_action_daily') }}
ORDER BY date, activity_kind
