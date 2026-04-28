{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_explorer', 'granularity:daily'],
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
                        "description": "Withdrawal credential (32-byte hex) — per-day cross-sectional APY quantiles across the validators under this credential, plus trailing 7d/30d medians of the credential's balance-weighted daily APY."
                    },
                    {
                        "name": "date_from",
                        "column": "date",
                        "operator": ">=",
                        "type": "date",
                        "description": "Inclusive lower bound on date"
                    },
                    {
                        "name": "date_to",
                        "column": "date",
                        "operator": "<=",
                        "type": "date",
                        "description": "Inclusive upper bound on date"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 1000,
                    "max_limit": 10000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "date", "direction": "DESC"}
                ]
            }
        }
    )
}}

SELECT * FROM {{ ref('fct_consensus_validators_explorer_apy_dist_daily') }}
