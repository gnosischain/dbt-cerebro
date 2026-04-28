{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_balance_history', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "start_date", "column": "date", "operator": ">=", "type": "date"},
          {"name": "end_date", "column": "date", "operator": "<=", "type": "date"}
        ],
        "pagination": {"enabled": true, "default_limit": 1000, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "date", "direction": "ASC"}]
      }
    }
  )
}}

SELECT
  address,
  date,
  total_balance_usd,
  tokens_held,
  native_or_wrapped_xdai_balance,
  priced_balance_usd,
  priced_tokens_held
FROM {{ ref('fct_execution_account_balance_history_daily') }}

