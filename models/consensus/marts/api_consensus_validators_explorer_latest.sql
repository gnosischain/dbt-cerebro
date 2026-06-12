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
                        "description": "Withdrawal credential (32-byte hex) — aggregates KPIs across every validator sharing this credential"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 100,
                    "max_limit": 1000,
                    "response": "envelope"
                }
            }
        }
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_consensus_validators_income_daily') }}) AS as_of_date
FROM (
SELECT * FROM {{ ref('fct_consensus_validators_explorer_latest') }}
) AS sub
