{{ 
    config(
        materialized='view',
        tags=['production','esg', 'tier1', 'api:energy_and_emissions_annual', 'granularity:daily']
    )
}}

SELECT 
    date
    ,label
    ,mean_val
    ,lower_95
    ,upper_95
    ,lower_90
    ,upper_90
FROM (
    SELECT 
        date
        ,'Energy (MWh)' AS label
        ,annual_energy_Mwh_projected AS mean_val
        ,annual_energy_mwh_lower_95 AS lower_95
        ,annual_energy_mwh_upper_95 AS upper_95
        ,annual_energy_mwh_lower_90 AS lower_90
        ,annual_energy_mwh_upper_90 AS upper_90
    FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    UNION ALL
    SELECT 
        date
        ,'CO2e (tonnes)' AS label
        ,annual_co2_tonnes_projected AS mean_val
        ,annual_co2_tonnes_lower_95 AS lower_95
        ,annual_co2_tonnes_upper_95 AS upper_95
        ,annual_co2_tonnes_lower_90 AS lower_90
        ,annual_co2_tonnes_upper_90 AS upper_90
    FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
)
ORDER BY date, label
