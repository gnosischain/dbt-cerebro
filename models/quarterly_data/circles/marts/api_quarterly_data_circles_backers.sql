{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:circles_backers', 'granularity:quarterly'],
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

-- Quarterly CURRENTLY-TRUSTED backers, as of quarter end (revocation-aware).
-- Takes the currently-trusted count on the latest available day within each
-- quarter (= the quarter's last calendar day for a closed quarter). This is the
-- end-of-quarter snapshot the quarterly report uses. Distinct from the ever-backed
-- cumulative series (api:circles_v2_backers_cumulative), which never drops backers
-- whose trust was later revoked.

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(currently_trusted_backers, date) AS total_backers
FROM {{ ref('fct_execution_circles_v2_backers_current_daily') }}
WHERE date < today()
GROUP BY quarter
ORDER BY quarter
