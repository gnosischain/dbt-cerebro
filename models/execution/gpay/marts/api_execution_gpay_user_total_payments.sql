{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_total_payments','granularity:all_time'],
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
    sum(activity_count) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Payment'
GROUP BY wallet_address
