{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_explorer', 'granularity:latest'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["withdrawal_credentials"],
                "parameters": [
                    {
                        "name": "withdrawal_credentials",
                        "column": "withdrawal_credentials",
                        "operator": "=",
                        "type": "string",
                        "case": "lower",
                        "description": "Withdrawal credential (32-byte hex) — returns every validator sharing this credential"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 100,
                    "max_limit": 5000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "validator_index", "direction": "ASC"}
                ],
                "sortable_fields": [
                    "validator_index",
                    "balance_gno",
                    "consensus_income_amount_30d_gno",
                    "proposed_blocks_count_lifetime",
                    "total_income_estimated_gno"
                ]
            }
        }
    )
}}

SELECT * FROM {{ ref('fct_consensus_validators_explorer_members_table') }}
