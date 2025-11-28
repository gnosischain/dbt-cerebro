{{ 
    config(
        materialized='view',
        tags=['production','esg','carbon_emissions', 'tier0', 'api: carbon_emissions_annualised_latest']
    )
}}

SELECT
    annual_co2_tonnes_projected
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
ORDER BY date DESC 
LIMIT 1

