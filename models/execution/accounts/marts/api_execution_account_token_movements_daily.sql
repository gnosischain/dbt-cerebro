{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_token_movements', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "counterparty", "column": "counterparty", "operator": "=", "type": "string", "case": "lower"},
          {"name": "symbol", "column": "symbol", "operator": "=", "type": "string"},
          {"name": "direction", "column": "direction", "operator": "=", "type": "string"},
          {"name": "start_date", "column": "date", "operator": ">=", "type": "date"},
          {"name": "end_date", "column": "date", "operator": "<=", "type": "date"}
        ],
        "pagination": {"enabled": true, "default_limit": 250, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  date,
  token_address,
  symbol,
  token_class,
  address,
  counterparty,
  direction,
  net_amount_raw,
  gross_amount_raw,
  transfer_count
FROM {{ ref('fct_execution_account_token_movements_daily') }}

