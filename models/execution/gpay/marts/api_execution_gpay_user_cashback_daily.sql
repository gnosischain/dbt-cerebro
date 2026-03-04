{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_cashback_daily','granularity:daily'],
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
    wallet_address,
    date,
    round(toFloat64(amount), 6) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Cashback'
ORDER BY date
