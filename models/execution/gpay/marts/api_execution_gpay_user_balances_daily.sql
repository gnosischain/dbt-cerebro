{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_balances_daily','granularity:daily'],
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
            "name": "token",
            "column": "token",
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
    address AS wallet_address,
    date,
    symbol AS label,
    symbol AS token,
    round(toFloat64(balance), 6)     AS value_native,
    round(toFloat64(balance_usd), 2) AS value_usd
FROM {{ ref('int_execution_gpay_balances_daily') }}
ORDER BY date
