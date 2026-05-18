{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_backers_cumulative', 'granularity:daily']
    )
}}

-- Daily cumulative trust-defined backers, latest day excluded.

SELECT
    date,
    new_backers,
    cumulative_backers
FROM {{ ref('fct_execution_circles_v2_backers_cumulative_daily') }}
WHERE date < today()
ORDER BY date
