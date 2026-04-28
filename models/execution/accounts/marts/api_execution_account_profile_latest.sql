{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_profile', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20}
        ],
        "pagination": {"enabled": true, "default_limit": 20, "max_limit": 200, "response": "envelope"}
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_account_profile_latest') }}

