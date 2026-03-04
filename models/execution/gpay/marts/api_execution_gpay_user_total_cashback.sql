{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_total_cashback','granularity:all_time'],
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
            "max_items": 50,
            "description": "Wallet address(es)"
          }
        ]
      }
    }
  )
}}

SELECT
    wallet_address,
    round(toFloat64(sum(amount)), 6) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Cashback'
GROUP BY wallet_address
