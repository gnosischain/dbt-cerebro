{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:carbon_emissions', 'granularity:quarterly'],
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
    argMax(annual_co2_tonnes_projected, date) AS co2_tonnes_yr,
    argMax(is_estimated, date) AS is_estimated
FROM {{ ref('int_quarterly_esg_carbon_footprint_with_fallback') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY quarter
ORDER BY quarter
