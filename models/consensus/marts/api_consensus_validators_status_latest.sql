{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_status', 'granularity:latest'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["withdrawal_credentials", "pubkey", "validator_index", "withdrawal_address"],
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
                    },
                    {
                        "name": "validator_index",
                        "column": "validator_index",
                        "operator": "IN",
                        "type": "integer_list",
                        "max_items": 200,
                        "description": "Validator index / indices"
                    },
                    {
                        "name": "withdrawal_address",
                        "column": "withdrawal_address",
                        "operator": "IN",
                        "type": "string_list",
                        "case": "lower",
                        "max_items": 200,
                        "description": "20-byte withdrawal address(es) (derived from 0x01/0x02 credentials)"
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
                    "balance",
                    "effective_balance",
                    "status",
                    "activation_epoch",
                    "exit_epoch",
                    "withdrawable_epoch"
                ]
            }
        }
    )
}}

SELECT
    *
FROM {{ ref('fct_consensus_validators_status_latest') }}
