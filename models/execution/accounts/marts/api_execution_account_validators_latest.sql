{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'validators', 'tier1', 'api:account_validators', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["withdrawal_address", "withdrawal_credentials"],
        "parameters": [
          {"name": "withdrawal_address", "column": "withdrawal_address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "withdrawal_credentials", "column": "withdrawal_credentials", "operator": "=", "type": "string", "case": "lower"}
        ],
        "pagination": {"enabled": true, "default_limit": 100, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "validator_index", "direction": "ASC"}]
      }
    }
  )
}}

SELECT
  validator_index,
  pubkey,
  withdrawal_credentials,
  withdrawal_address,
  status,
  slashed,
  balance_gno,
  effective_balance_gno,
  consensus_income_amount_30d_gno,
  total_income_estimated_gno,
  proposed_blocks_count_lifetime,
  latest_date
FROM {{ ref('fct_consensus_validators_explorer_members_table') }}
