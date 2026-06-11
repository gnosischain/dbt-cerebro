{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_lifetime_metrics','granularity:all_time'],
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

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT *
FROM {{ ref('fct_execution_gpay_user_lifetime_metrics') }}
) AS sub
