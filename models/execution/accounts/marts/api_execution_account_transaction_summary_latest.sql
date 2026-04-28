{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_transaction_summary', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20}
        ],
        "pagination": {"enabled": true, "default_limit": 50, "max_limit": 500, "response": "envelope"}
      }
    }
  )
}}

SELECT
  address,
  first_activity_date,
  last_activity_date,
  active_days,
  token_transfer_count,
  inbound_transfer_count,
  outbound_transfer_count,
  counterparty_count,
  token_count_moved,
  inbound_gross_amount_raw,
  outbound_gross_amount_raw
FROM {{ ref('fct_execution_account_transaction_summary_latest') }}

