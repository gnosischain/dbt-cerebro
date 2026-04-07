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
                    "max_limit": 5000
                },
                "sort": [
                    {"column": "validator_index", "direction": "ASC"}
                ]
            }
        }
    )
}}

SELECT
    slot,
    validator_index,
    balance,
    status,
    lower(pubkey) AS pubkey,
    lower(withdrawal_credentials) AS withdrawal_credentials,
    effective_balance,
    slashed,
    activation_eligibility_epoch,
    activation_epoch,
    exit_epoch,
    withdrawable_epoch,
    slot_timestamp
FROM {{ ref('stg_consensus__validators_all') }} FINAL
WHERE slot = (SELECT MAX(slot) FROM {{ ref('stg_consensus__validators_all') }} )
ORDER BY validator_index
