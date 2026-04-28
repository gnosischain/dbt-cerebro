{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_recent_transactions', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "counterparty", "column": "counterparty", "operator": "=", "type": "string", "case": "lower"}
        ],
        "pagination": {"enabled": true, "default_limit": 100, "max_limit": 1000, "response": "envelope"},
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  date,
  address,
  counterparty,
  symbol,
  token_address,
  direction,
  transfer_count,
  net_amount_raw,
  gross_amount_raw
FROM {{ ref('fct_execution_account_token_movements_daily') }}

