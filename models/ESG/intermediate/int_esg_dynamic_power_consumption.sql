{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='(date, node_category, country_code)',
        order_by='(date, node_category, country_code)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        },
        tags=['production','esg','power_consumption']
    )
}}

WITH node_distribution AS (
    SELECT
        date,
        node_category,
        country_code,
        country_name,
        region,
        sub_region,
        country_code_alpha3,
        estimated_total_nodes,  
        nodes_lower_95,           
        nodes_upper_95          
    FROM {{ ref('int_esg_node_geographic_distribution') }} n
    {% if is_incremental() %}
        WHERE n.date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Get client efficiency from upstream client distribution model
client_efficiency_by_category AS (
    SELECT
        date,
        node_category,
        
        -- Weighted average client efficiency for this category
        SUM(category_client_percentage / 100.0 * client_efficiency_factor) AS avg_client_efficiency,
        
        -- Client diversity count for resilience bonus
        COUNT(DISTINCT client_type) AS client_diversity,
        
        -- Total estimated client nodes for validation
        SUM(estimated_client_nodes) AS total_client_nodes,
        
        -- Client breakdown for diagnostics
        groupArray((client_type, category_client_percentage, estimated_client_nodes)) AS client_breakdown
        
    FROM {{ ref('int_esg_node_client_distribution') }} ncd
    {% if is_incremental() %}
        WHERE ncd.date > (SELECT MAX(date) FROM {{ this }}) - INTERVAL 1 DAY
    {% endif %}
    GROUP BY date, node_category
),

-- Base power calculations by hardware tier with CCRI empirical values
power_per_category AS (
    SELECT
        nd.date,
        nd.node_category,
        nd.country_code,
        nd.country_name,
        nd.region,
        nd.sub_region,
        nd.country_code_alpha3,
        nd.estimated_total_nodes,
        nd.nodes_lower_95,
        nd.nodes_upper_95,
        
        -- CCRI empirical power consumption (Watts per node)
        CASE nd.node_category
            WHEN 'home_staker' THEN 22.0        -- CCRI Tier 4: mean of 16.56-25.97W
            WHEN 'professional_operator' THEN 48.0  -- CCRI Tier 5: mean of 36.82-59.95W
            WHEN 'cloud_hosted' THEN 155.0      -- CCRI Tier 6: mean of 139.90-186.76W
            ELSE 50.0
        END AS base_power_watts,
        
        -- Standard deviation based on CCRI measurement ranges
        CASE nd.node_category
            WHEN 'home_staker' THEN 3.3        -- ~15% of 22W (conservative uncertainty)
            WHEN 'professional_operator' THEN 7.2  -- ~15% of 48W
            WHEN 'cloud_hosted' THEN 23.0       -- ~15% of 155W
            ELSE 7.5
        END AS power_std_watts,
        
        -- Data source for traceability
        CASE nd.node_category
            WHEN 'home_staker' THEN 'CCRI_Tier4_Empirical'
            WHEN 'professional_operator' THEN 'CCRI_Tier5_Empirical'
            WHEN 'cloud_hosted' THEN 'CCRI_Tier6_Empirical'
            ELSE 'CCRI_Default'
        END AS power_source,
        
        -- Measurement confidence (CCRI empirical data is high quality)
        0.85 AS measurement_confidence,
        
        -- PUE by category  
        CASE nd.node_category
            WHEN 'home_staker' THEN 1.0         -- No datacenter overhead
            WHEN 'professional_operator' THEN 1.58  -- Traditional datacenter
            WHEN 'cloud_hosted' THEN 1.15       -- Efficient cloud datacenter
            ELSE 1.1
        END AS pue_factor,
        
        -- Client efficiency from upstream model
        COALESCE(ce.avg_client_efficiency, 1.0) AS client_efficiency_multiplier,
        
        -- Diversity bonus (more client types = better resilience)
        CASE 
            WHEN ce.client_diversity > 0 THEN 0.95 + 0.05 * least(4, ce.client_diversity) / 4.0
            ELSE 1.0
        END AS diversity_bonus,
        
        ce.client_breakdown,
        ce.total_client_nodes
        
    FROM node_distribution nd
    LEFT JOIN client_efficiency_by_category ce ON nd.date = ce.date AND nd.node_category = ce.node_category
),

-- Apply all efficiency factors
final_power_calculations AS (
    SELECT
        p.*,
        
        -- Final power per node with all efficiency factors
        p.base_power_watts * p.client_efficiency_multiplier * p.diversity_bonus AS avg_power_watts_per_node,
        p.power_std_watts * p.client_efficiency_multiplier * p.diversity_bonus AS power_std_dev_per_node,
        
        -- Daily energy consumption (kWh)
        p.estimated_total_nodes * 
        p.base_power_watts * 
        p.client_efficiency_multiplier * 
        p.diversity_bonus * 
        24.0 / 1000.0 AS daily_energy_kwh_mean

    FROM power_per_category p
),

-- Carbon intensity lookup with robust fallback
carbon_intensity_lookup AS (
    SELECT DISTINCT
        p.date,
        p.country_code_alpha3,
        
        -- Robust fallback hierarchy
        COALESCE(
            ci_country.carbon_intensity_mean,
            ci_world.carbon_intensity_mean,
            450.0
        ) AS carbon_intensity_gco2_kwh,
        
        COALESCE(
            ci_country.carbon_intensity_std,
            ci_world.carbon_intensity_std, 
            45.0
        ) AS carbon_intensity_std_gco2_kwh,
        
        CASE 
            WHEN ci_country.carbon_intensity_mean IS NOT NULL THEN 'country_specific'
            WHEN ci_world.carbon_intensity_mean IS NOT NULL THEN 'world_average'
            ELSE 'conservative_default'
        END AS carbon_intensity_source
        
    FROM final_power_calculations p
    LEFT JOIN {{ ref('int_esg_carbon_intensity_ensemble') }} ci_country
        ON p.country_code_alpha3 = ci_country.country_code
        AND ci_country.month_date = toStartOfMonth(p.date)
        AND p.country_code_alpha3 IS NOT NULL
        AND p.country_code_alpha3 != ''
    LEFT JOIN {{ ref('int_esg_carbon_intensity_ensemble') }} ci_world
        ON ci_world.country_code = 'WORLD'
        AND ci_world.month_date = toStartOfMonth(p.date)
),

-- Final calculations with carbon emissions
final_calculations AS (
    SELECT
        p.date AS date,
        p.node_category AS node_category,
        p.country_code AS country_code,
        p.country_code_alpha3 AS country_code_alpha3,
        p.country_name AS country_name,
        p.region AS region,
        p.sub_region AS sub_region,
        p.estimated_total_nodes AS estimated_total_nodes,
        p.nodes_lower_95 AS nodes_lower_95,
        p.nodes_upper_95 AS nodes_upper_95,
        p.avg_power_watts_per_node AS avg_power_watts_per_node,
        p.power_std_dev_per_node AS power_std_dev_per_node,
        p.daily_energy_kwh_mean AS daily_energy_kwh_mean,
        p.pue_factor AS pue_factor,
        p.client_efficiency_multiplier AS client_efficiency,
        p.diversity_bonus,
        
        -- CCRI source tracking
        p.power_source,
        p.measurement_confidence,
        p.base_power_watts AS ccri_base_power_watts,
        
        -- Carbon intensity from lookup
        ci.carbon_intensity_gco2_kwh,
        ci.carbon_intensity_std_gco2_kwh,
        ci.carbon_intensity_source,
        
        -- CO2 calculations
        p.daily_energy_kwh_mean * p.pue_factor * ci.carbon_intensity_gco2_kwh / 1000.0 AS daily_co2_kg_mean,
        
        -- CO2 standard deviation
        sqrt(
            pow(p.daily_energy_kwh_mean * p.pue_factor * ci.carbon_intensity_std_gco2_kwh / 1000.0, 2) +
            pow(p.power_std_dev_per_node * 24 / 1000.0 * p.pue_factor * ci.carbon_intensity_gco2_kwh / 1000.0, 2)
        ) AS daily_co2_kg_std,
        
        -- Diagnostics
        p.client_breakdown,
        p.total_client_nodes
        
    FROM final_power_calculations p
    JOIN carbon_intensity_lookup ci
        ON p.date = ci.date
        AND COALESCE(p.country_code_alpha3, '') = COALESCE(ci.country_code_alpha3, '')
)

SELECT
    date,
    node_category,
    country_code,
    country_name,
    region,
    estimated_total_nodes,
    nodes_lower_95,
    nodes_upper_95,
    
    -- Power consumption metrics
    round(avg_power_watts_per_node, 2) AS avg_power_watts_per_node,
    round(power_std_dev_per_node, 2) AS power_std_dev_per_node,
    round(daily_energy_kwh_mean, 2) AS daily_energy_kwh_mean,
    round(pue_factor, 3) AS pue_mean,
    round(client_efficiency, 3) AS client_efficiency_factor,
    round(diversity_bonus, 3) AS diversity_bonus,
    
    -- Carbon emissions
    round(daily_co2_kg_mean, 4) AS daily_co2_kg_mean,
    round(daily_co2_kg_std, 4) AS daily_co2_kg_std,
    round(carbon_intensity_gco2_kwh, 2) AS carbon_intensity_gco2_kwh,
    round(carbon_intensity_std_gco2_kwh, 2) AS carbon_intensity_std_gco2_kwh,
    
    -- Confidence intervals
    round(greatest(0, daily_co2_kg_mean - 1.96 * daily_co2_kg_std), 4) AS daily_co2_kg_lower_95,
    round(daily_co2_kg_mean + 1.96 * daily_co2_kg_std, 4) AS daily_co2_kg_upper_95,
    
    -- CCRI data quality and source tracking
    carbon_intensity_source,
    round(ccri_base_power_watts, 2) AS ccri_base_power_watts,
    power_source,
    round(measurement_confidence, 3) AS measurement_confidence,
    
    -- Performance comparison with previous estimates
    round(
        100.0 * (avg_power_watts_per_node - CASE node_category
            WHEN 'home_staker' THEN 75.0
            WHEN 'professional_operator' THEN 200.0  
            WHEN 'cloud_hosted' THEN 110.0
            ELSE 100.0
        END) / CASE node_category
            WHEN 'home_staker' THEN 75.0
            WHEN 'professional_operator' THEN 200.0
            WHEN 'cloud_hosted' THEN 110.0  
            ELSE 100.0
        END, 1
    ) AS power_reduction_vs_previous_pct,
    
    -- Diagnostics
    toJSONString(client_breakdown) AS client_breakdown_json,
    total_client_nodes AS debug_total_clients,
    
    -- Metadata
    now() AS calculated_at

FROM final_calculations
WHERE estimated_total_nodes > 0