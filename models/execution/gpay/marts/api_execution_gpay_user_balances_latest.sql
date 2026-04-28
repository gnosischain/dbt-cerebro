{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gpay', 'tier0', 'api:gpay_user_balances_latest', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["wallet_address"],
        "parameters": [
          {"name": "wallet_address", "column": "wallet_address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "token", "column": "token", "operator": "=", "type": "string"}
        ],
        "pagination": {"enabled": true, "default_limit": 100, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "value_usd", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  wallet_address,
  token,
  value_usd,
  value_native,
  date
FROM {{ ref('fct_execution_gpay_user_balances_latest') }}

