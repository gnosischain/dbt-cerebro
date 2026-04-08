{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_status', 'granularity:latest'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["withdrawal_credentials", "pubkey"],
                "parameters": [
                    {
                        "name": "withdrawal_credentials",
                        "column": "withdrawal_credentials",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "Withdrawal credential value(s)"
                    },
                    {
                        "name": "pubkey",
                        "column": "pubkey",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "Validator public key(s)"
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
                ]
            }
        }
    )
}}

SELECT
    *
FROM {{ ref('fct_consensus_validators_status_latest') }}
