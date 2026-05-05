{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:energy_consumption', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(annual_energy_Mwh_projected, date) AS energy_mwh_yr,
    argMax(is_estimated, date) AS is_estimated
FROM {{ ref('int_quarterly_esg_carbon_footprint_with_fallback') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY quarter
ORDER BY quarter
