{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_apy', 'granularity:daily'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "date_from", "column": "date", "operator": ">=", "type": "date", "description": "Inclusive lower bound on date"},
                    {"name": "date_to", "column": "date", "operator": "<=", "type": "date", "description": "Inclusive upper bound on date"}
                ],
                "pagination": {"enabled": true, "default_limit": 2000, "max_limit": 10000, "response": "envelope"},
                "sort": [{"column": "date", "direction": "DESC"}]
            }
        }
    )
}}

SELECT * FROM {{ ref('fct_consensus_validators_apy_mean_daily') }}
