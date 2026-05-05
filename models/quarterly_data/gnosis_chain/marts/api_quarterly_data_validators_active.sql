{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:validators_active', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(cnt, date) AS validators_active
FROM {{ ref('int_consensus_validators_status_daily') }}
WHERE status = 'active_ongoing'
GROUP BY quarter
ORDER BY quarter
