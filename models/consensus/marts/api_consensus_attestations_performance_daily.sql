{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:attestations_performance', 'granularity:daily'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
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
                    "default_limit": 100,
                    "max_limit": 5000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "date", "direction": "DESC"}
                ],
                "sortable_fields": [
                    "date",
                    "attestations_total",
                    "avg_inclusion_delay",
                    "p50_inclusion_delay"
                ]
            }
        }
    )
}}

SELECT
    date
    ,attestations_total
    ,avg_inclusion_delay
    ,p50_inclusion_delay
    ,pct_inclusion_distance_1
    ,pct_inclusion_distance_le_2
    ,pct_inclusion_distance_gt_1
FROM {{ ref('fct_consensus_attestations_performance_daily') }}
