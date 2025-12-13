{{ 
    config(
        materialized='view',
        tags=['production','esg', 'tier1', 'api:carbon_emissions_distribution', 'granularity:daily']
    )
}}


SELECT
    date,
    daily_co2_kg_mean AS value,
    daily_co2_kg_lower_95 AS lower_95,
    daily_co2_kg_upper_95 AS upper_95,
    daily_co2_kg_lower_90 AS lower_90,
    daily_co2_kg_upper_90 AS upper_90,
    
    -- Moving averages for smoothing
    AVG(daily_co2_kg_mean) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_value,
    AVG(daily_co2_kg_lower_95) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_lower_95,
    AVG(daily_co2_kg_upper_95) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_upper_95,
    
    -- Month-to-date statistics
    AVG(daily_co2_kg_mean) OVER (PARTITION BY toStartOfMonth(date) ORDER BY date) AS mtd_avg,
    SUM(daily_co2_kg_mean) OVER (PARTITION BY toStartOfMonth(date) ORDER BY date) AS mtd_total
    
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
