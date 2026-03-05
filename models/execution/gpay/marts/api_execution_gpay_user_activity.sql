{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_activity'],
    meta={
      "api": {
        "methods": ["GET","POST"],
        "allow_unfiltered": false,
        "require_any_of": ["wallet_address"],
        "parameters": [
          {
            "name": "wallet_address",
            "column": "wallet_address",
            "operator": "IN",
            "type": "string_list",
            "case": "lower",
            "max_items": 20,
            "description": "Wallet address(es)"
          },
          {
            "name": "action",
            "column": "action",
            "operator": "=",
            "type": "string",
            "description": "Action type (Payment, Cashback, Fiat Top Up, etc.)"
          },
          {
            "name": "symbol",
            "column": "symbol",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
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
    transaction_hash,
    wallet_address,
    block_timestamp AS timestamp,
    date,
    action,
    symbol,
    direction,
    round(toFloat64(amount), 6)     AS amount,
    round(toFloat64(amount_usd), 2) AS amount_usd,
    counterparty
FROM {{ ref('int_execution_gpay_activity') }}
ORDER BY block_timestamp DESC
