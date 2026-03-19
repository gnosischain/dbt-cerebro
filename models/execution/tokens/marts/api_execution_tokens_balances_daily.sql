{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:token_balances','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET","POST"],
        "allow_unfiltered": false,
        "require_any_of": ["symbol","address"],
        "parameters": [
          {
            "name": "symbol",
            "column": "symbol",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
          },
          {
            "name": "address",
            "column": "address",
            "operator": "IN",
            "type": "string_list",
            "case": "lower",
            "max_items": 200,
            "description": "Wallet address list"
          },
          {
            "name": "start_date",
            "column": "date",
            "operator": ">=",
            "type": "date",
            "description": "Inclusive start date"
          },
          {
            "name": "end_date",
            "column": "date",
            "operator": "<=",
            "type": "date",
            "description": "Inclusive end date"
          }
        ],
        "pagination": {
          "enabled": true,
          "default_limit": 100,
          "max_limit": 5000
        },
        "sort": [
          {"column": "date", "direction": "DESC"}
        ]
      }
    }
  )
}}

SELECT
    date,
    token_address,
    symbol,
    address,
    balance,
    balance_usd
FROM {{ ref('int_execution_tokens_balances_daily') }}