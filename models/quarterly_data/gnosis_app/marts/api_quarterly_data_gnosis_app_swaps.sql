{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gnosis_app_swaps', 'granularity:quarterly'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "quarter_from", "column": "quarter", "operator": ">=", "type": "date", "description": "Inclusive lower bound on quarter start date (e.g. 2024-01-01 for 2024-Q1)"},
                    {"name": "quarter_to", "column": "quarter", "operator": "<=", "type": "date", "description": "Inclusive upper bound on quarter start date"}
                ],
                "pagination": {"enabled": true, "default_limit": 200, "max_limit": 1000, "response": "envelope"},
                "sort": [{"column": "quarter", "direction": "DESC"}]
            }
        }
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    sum(n_swaps) AS swaps
FROM {{ ref('fct_execution_gnosis_app_swaps_monthly') }}
GROUP BY quarter
ORDER BY quarter
