{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_token_balances_latest', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "symbol", "column": "symbol", "operator": "=", "type": "string"}
        ],
        "pagination": {"enabled": true, "default_limit": 100, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "balance_usd", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  address,
  date,
  token_address,
  symbol,
  token_class,
  balance_raw,
  balance,
  balance_usd
FROM {{ ref('fct_execution_account_token_balances_latest') }}

