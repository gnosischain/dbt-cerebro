{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:circles_total_supply', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total_supply, date) AS total_supply,
    argMax(total_demurraged_supply, date) AS total_supply_demurraged
FROM {{ ref('fct_execution_circles_v2_total_supply_daily') }}
WHERE date < today()
GROUP BY quarter
ORDER BY quarter
