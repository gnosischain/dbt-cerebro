{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:nodes_estimated', 'granularity:quarterly'],
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
    argMax(total_estimated, date) AS nodes_estimated,
    argMax(total_lower_95, date) AS nodes_lower_95,
    argMax(total_upper_95, date) AS nodes_upper_95
FROM (
    SELECT
        date,
        sum(estimated_total_nodes) AS total_estimated,
        sum(nodes_lower_95) AS total_lower_95,
        sum(nodes_upper_95) AS total_upper_95
    FROM {{ ref('int_esg_node_classification') }}
    WHERE date < today()
    GROUP BY date
)
GROUP BY quarter
ORDER BY quarter
