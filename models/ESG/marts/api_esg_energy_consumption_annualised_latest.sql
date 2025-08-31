{{ 
    config(
        materialized='view',
        tags=['production','esg','energy_consumption']
    )
}}


SELECT
    annual_energy_Mwh_projected
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
ORDER BY date DESC 
LIMIT 1