{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:carbon_emissions', 'granularity:quarterly']
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
