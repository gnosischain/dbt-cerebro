{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:consolidations', 'granularity:daily'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "date_from", "column": "date", "operator": ">=", "type": "date", "description": "Inclusive lower bound on date"},
                    {"name": "date_to", "column": "date", "operator": "<=", "type": "date", "description": "Inclusive upper bound on date"},
                    {"name": "role", "column": "role", "operator": "=", "type": "string", "description": "One of self / source / target"}
                ],
                "pagination": {"enabled": true, "default_limit": 5000, "max_limit": 10000, "response": "envelope"},
                "sort": [{"column": "date", "direction": "DESC"}]
            }
        }
    )
}}

SELECT * FROM {{ ref('fct_consensus_consolidations_daily') }}
