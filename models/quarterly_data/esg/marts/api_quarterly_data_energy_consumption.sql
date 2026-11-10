{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:energy_consumption', 'granularity:quarterly'],
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

-- Until 2026-11 this mart also returned is_estimated (true when the quarter's last day
-- used forward-filled carbon intensity). The ESG chain now waits for Ember's
-- publication, so no quarter is estimated and the column was dropped.

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(annual_energy_Mwh_projected, date) AS energy_mwh_yr
FROM {{ ref('int_quarterly_esg_carbon_footprint_with_fallback') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY quarter
ORDER BY quarter
