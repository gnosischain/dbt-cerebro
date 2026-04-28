{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_search', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["search_key"],
        "parameters": [
          {"name": "search_key", "column": "search_key", "operator": "=", "type": "string", "case": "lower"},
          {"name": "address", "column": "address", "operator": "=", "type": "string", "case": "lower"},
          {"name": "result_type", "column": "result_type", "operator": "=", "type": "string"}
        ],
        "pagination": {"enabled": true, "default_limit": 20, "max_limit": 100, "response": "envelope"}
      }
    }
  )
}}

SELECT
  search_key,
  result_type,
  address,
  display_label,
  subtitle,
  badges,
  validator_index,
  withdrawal_credentials,
  score_base
FROM {{ ref('fct_execution_account_search_index') }}

