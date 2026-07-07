{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:circles_active_minters', 'granularity:quarterly'],
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

-- Quarterly PEAK of blacklist-excluded Active Minters.
-- The +80% bucket (cohort_order = 6) of fct_execution_circles_v2_minter_cohort_daily
-- already excludes the bot/sybil blacklist (stg_crawlers_data__circles_blacklisted)
-- and is the blacklist-excluded equivalent of the Active Minters KPI. The
-- quarterly datapoint is the PEAK daily count within the quarter, matching the
-- Dune circles-v2-kpis "active minters (peak)" presentation. The peak is used
-- (not end-of-quarter) because it is the reported statistic and is robust to
-- recent-tail data settling.

SELECT
    toStartOfQuarter(date) AS quarter,
    max(cnt)               AS active_minters_peak
FROM {{ ref('fct_execution_circles_v2_minter_cohort_daily') }}
WHERE cohort_order = 6
  AND date < today()
GROUP BY quarter
ORDER BY quarter
