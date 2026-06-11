{{ 
    config(
        materialized='view',
        tags=['production','esg', 'tier0', 'api:energy_consumption_annualised', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}) AS as_of_date
FROM (
SELECT
    annual_energy_Mwh_projected
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
ORDER BY date DESC 
LIMIT 1
) AS sub
