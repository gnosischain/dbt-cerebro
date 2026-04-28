{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'gnosis_app', 'tier1', 'api:gnosis_app_user_activity', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "activity_kind", "column": "activity_kind", "operator": "=", "type": "string"},
          {"name": "start_date", "column": "date", "operator": ">=", "type": "date"},
          {"name": "end_date", "column": "date", "operator": "<=", "type": "date"}
        ],
        "pagination": {"enabled": true, "default_limit": 200, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  date,
  address,
  activity_kind,
  n_events,
  amount_usd
FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}

