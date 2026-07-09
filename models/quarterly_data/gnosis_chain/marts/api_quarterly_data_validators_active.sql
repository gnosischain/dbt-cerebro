{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:validators_active', 'granularity:quarterly'],
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

-- Active validators = the full active set (active_ongoing + active_exiting) at
-- quarter-end, matching public beacon explorers (dora / beaconcha). Exiting
-- validators still attest and remain staked until their exit completes, so they
-- are part of the active set. The two statuses are summed PER DAY before the
-- quarter-end argMax: a bare argMax(cnt, date) over both status rows would pick
-- one row arbitrarily on the last day rather than their sum.
WITH per_day AS (
    SELECT
        date,
        sum(cnt) AS active_cnt
    FROM {{ ref('int_consensus_validators_status_daily') }}
    WHERE status IN ('active_ongoing', 'active_exiting')
    GROUP BY date
)

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(active_cnt, date) AS validators_active
FROM per_day
GROUP BY quarter
ORDER BY quarter
