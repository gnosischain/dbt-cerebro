{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:nodes_observed', 'granularity:quarterly'],
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
    toStartOfQuarter(date) AS quarter,
    argMax(total_observed, date) AS nodes_observed
FROM (
    SELECT date, sum(observed_nodes) AS total_observed
    FROM {{ ref('int_esg_node_classification') }}
    WHERE date < today()
    GROUP BY date
)
GROUP BY quarter
ORDER BY quarter
