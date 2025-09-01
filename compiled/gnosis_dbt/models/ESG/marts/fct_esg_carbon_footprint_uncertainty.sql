

WITH node_country_distribution AS (
    -- Get node distribution by country and category with their carbon intensities
    SELECT
        date,
        node_category,
        country_code,
        estimated_total_nodes,
        nodes_lower_95,
        nodes_upper_95,
        carbon_intensity_gco2_kwh,
        daily_energy_kwh_mean,
        avg_power_watts_per_node,
        power_std_dev_per_node,
        daily_co2_kg_mean,
        daily_co2_kg_std
    FROM `dbt`.`int_esg_dynamic_power_consumption`
    
        WHERE date > (SELECT MAX(date) FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`)
    
),

-- Calculate network effective CIF from geographic distribution
network_effective_cif AS (
    SELECT
        date,
        -- Network Effective CIF = Σ(nodes_in_country × country_CIF) / total_nodes
        SUM(estimated_total_nodes * carbon_intensity_gco2_kwh) / 
        NULLIF(SUM(estimated_total_nodes), 0) AS network_weighted_cif,
        
        -- Weighted standard deviation of network CIF
        -- Using variance formula: Var = Σ(w_i * (x_i - mean)²) / Σ(w_i)
        -- Then std = sqrt(var)
        SQRT(
            SUM(estimated_total_nodes * pow(carbon_intensity_gco2_kwh, 2)) / NULLIF(SUM(estimated_total_nodes), 0) -
            pow(SUM(estimated_total_nodes * carbon_intensity_gco2_kwh) / NULLIF(SUM(estimated_total_nodes), 0), 2)
        ) AS network_cif_std
    FROM node_country_distribution
    GROUP BY date
),

daily_power_data AS (
    -- Aggregate power consumption across all categories and countries
    SELECT
        date,
        node_category,
        
        -- Node counts with uncertainty
        SUM(estimated_total_nodes) AS category_nodes,
        SUM(nodes_lower_95) AS category_nodes_lower_95,
        SUM(nodes_upper_95) AS category_nodes_upper_95,
        
        -- Energy totals with uncertainty calculation
        SUM(daily_energy_kwh_mean) AS category_energy_kwh_mean,
        
        -- Energy standard deviation (propagate uncertainty from power and node count)
        SQRT(SUM(
            pow(power_std_dev_per_node * estimated_total_nodes * 24 / 1000.0, 2) + 
            pow(avg_power_watts_per_node * (nodes_upper_95 - nodes_lower_95) / 3.92 * 24 / 1000.0, 2)
        )) AS category_energy_kwh_std,
        
        -- Carbon totals
        SUM(daily_co2_kg_mean) AS category_co2_kg,
        SQRT(SUM(pow(daily_co2_kg_std, 2))) AS category_co2_kg_std,
        
        -- Weighted averages
        SUM(daily_energy_kwh_mean * estimated_total_nodes) / NULLIF(SUM(estimated_total_nodes), 0) AS weighted_avg_energy_per_node,
        SUM(daily_co2_kg_mean * estimated_total_nodes) / NULLIF(SUM(estimated_total_nodes), 0) AS weighted_avg_co2_per_node,
        
        -- Country count for this category
        COUNT(DISTINCT country_code) AS countries_represented
        
    FROM node_country_distribution
    
        WHERE date > (SELECT MAX(date) FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`)
    
    GROUP BY date, node_category
),

network_totals AS (
    -- Calculate network-wide totals with full uncertainty propagation
    SELECT
        date,
        
        -- Total network size with bounds
        SUM(category_nodes) AS total_estimated_nodes,
        SUM(category_nodes_lower_95) AS total_nodes_lower_95,
        SUM(category_nodes_upper_95) AS total_nodes_upper_95,
        
        -- Total energy consumption with uncertainty
        SUM(category_energy_kwh_mean) AS total_energy_kwh_mean,
        SQRT(SUM(pow(category_energy_kwh_std, 2))) AS total_energy_kwh_std,
        
        -- Total emissions with error propagation
        SUM(category_co2_kg) AS total_co2_kg_mean,
        SQRT(SUM(pow(category_co2_kg_std, 2))) AS total_co2_kg_std,
        
        -- Category breakdown for energy
        SUM(CASE WHEN node_category = 'home_staker' THEN category_energy_kwh_mean ELSE 0 END) AS home_staker_energy_kwh,
        SUM(CASE WHEN node_category = 'professional_operator' THEN category_energy_kwh_mean ELSE 0 END) AS professional_energy_kwh,
        SUM(CASE WHEN node_category = 'cloud_hosted' THEN category_energy_kwh_mean ELSE 0 END) AS cloud_energy_kwh,
        SUM(CASE WHEN node_category = 'unknown' THEN category_energy_kwh_mean ELSE 0 END) AS unknown_energy_kwh,
        
        -- Category breakdown for carbon
        SUM(CASE WHEN node_category = 'home_staker' THEN category_co2_kg ELSE 0 END) AS home_staker_co2_kg,
        SUM(CASE WHEN node_category = 'professional_operator' THEN category_co2_kg ELSE 0 END) AS professional_co2_kg,
        SUM(CASE WHEN node_category = 'cloud_hosted' THEN category_co2_kg ELSE 0 END) AS cloud_co2_kg,
        SUM(CASE WHEN node_category = 'unknown' THEN category_co2_kg ELSE 0 END) AS unknown_co2_kg,
        
        -- Node breakdown
        SUM(CASE WHEN node_category = 'home_staker' THEN category_nodes ELSE 0 END) AS home_staker_nodes,
        SUM(CASE WHEN node_category = 'professional_operator' THEN category_nodes ELSE 0 END) AS professional_nodes,
        SUM(CASE WHEN node_category = 'cloud_hosted' THEN category_nodes ELSE 0 END) AS cloud_nodes,
        SUM(CASE WHEN node_category = 'unknown' THEN category_nodes ELSE 0 END) AS unknown_nodes,
        
        -- Quality metrics
        COUNT(DISTINCT CASE WHEN category_nodes > 0 THEN node_category END) AS active_categories,
        MAX(countries_represented) AS max_countries_in_category
        
    FROM daily_power_data
    GROUP BY date
),

-- Add Chao-1 population estimates for comparison
chao1_comparison AS (
    SELECT
        nt.date AS date,
        nt.*,
        necif.network_weighted_cif,
        necif.network_cif_std,
        
        -- Link to Chao-1 estimates
        c.observed_successful_nodes AS chao1_observed,
        c.enhanced_total_reachable AS chao1_estimated,
        c.connection_success_rate_pct AS chao1_success_rate,
        c.reachable_discovery_coverage_pct AS chao1_coverage,
        
        -- Compare our estimates to Chao-1
        round(100.0 * nt.total_estimated_nodes / NULLIF(c.enhanced_total_reachable, 0), 1) AS node_estimate_vs_chao1_pct,
        
        -- Calculate scaling factor applied
        round(toFloat64(nt.total_estimated_nodes) / NULLIF(c.observed_successful_nodes, 0), 2) AS applied_scaling_factor
        
    FROM network_totals nt
    JOIN network_effective_cif necif ON nt.date = necif.date
    LEFT JOIN `dbt`.`int_esg_node_population_chao1` c
        ON c.observation_date = nt.date
),

enhanced_statistics AS (
    SELECT
        date,
        
        -- Node population metrics with bounds
        total_estimated_nodes,
        total_nodes_lower_95,
        total_nodes_upper_95,
        chao1_observed,
        chao1_estimated,
        chao1_success_rate,
        chao1_coverage,
        node_estimate_vs_chao1_pct,
        applied_scaling_factor,
        
        -- Network carbon intensity with uncertainty
        round(network_weighted_cif, 2) AS network_carbon_intensity_gco2_kwh,
        round(network_cif_std, 2) AS network_carbon_intensity_std,
        
        -- Daily energy metrics with full uncertainty bands
        round(total_energy_kwh_mean, 2) AS daily_energy_kwh_mean,
        round(total_energy_kwh_std, 2) AS daily_energy_kwh_std,
        
        -- Daily energy confidence intervals (95%)
        round(greatest(0, total_energy_kwh_mean - 1.96 * total_energy_kwh_std), 2) AS daily_energy_kwh_lower_95,
        round(total_energy_kwh_mean + 1.96 * total_energy_kwh_std, 2) AS daily_energy_kwh_upper_95,
        
        -- Daily energy confidence intervals (90%)
        round(greatest(0, total_energy_kwh_mean - 1.645 * total_energy_kwh_std), 2) AS daily_energy_kwh_lower_90,
        round(total_energy_kwh_mean + 1.645 * total_energy_kwh_std, 2) AS daily_energy_kwh_upper_90,
        
        -- Annual energy projections with uncertainty
        round(total_energy_kwh_mean * 365 / 1000, 2) AS annual_energy_mwh_mean,
        round(total_energy_kwh_std * sqrt(365) / 1000, 2) AS annual_energy_mwh_std,
        
        -- Annual energy confidence intervals (95%)
        round(greatest(0, (total_energy_kwh_mean * 365 / 1000) - 1.96 * (total_energy_kwh_std * sqrt(365) / 1000)), 2) AS annual_energy_mwh_lower_95,
        round((total_energy_kwh_mean * 365 / 1000) + 1.96 * (total_energy_kwh_std * sqrt(365) / 1000), 2) AS annual_energy_mwh_upper_95,
        
        -- Annual energy confidence intervals (90%)
        round(greatest(0, (total_energy_kwh_mean * 365 / 1000) - 1.645 * (total_energy_kwh_std * sqrt(365) / 1000)), 2) AS annual_energy_mwh_lower_90,
        round((total_energy_kwh_mean * 365 / 1000) + 1.645 * (total_energy_kwh_std * sqrt(365) / 1000), 2) AS annual_energy_mwh_upper_90,
        
        -- Carbon emissions (primary metrics)
        round(total_co2_kg_mean, 2) AS daily_co2_kg_mean,
        round(total_co2_kg_std, 2) AS daily_co2_kg_std,
        
        -- Daily CO2 confidence intervals (95%)
        round(greatest(0, total_co2_kg_mean - 1.96 * total_co2_kg_std), 2) AS daily_co2_kg_lower_95,
        round(total_co2_kg_mean + 1.96 * total_co2_kg_std, 2) AS daily_co2_kg_upper_95,
        
        -- Daily CO2 confidence intervals (90%)
        round(greatest(0, total_co2_kg_mean - 1.645 * total_co2_kg_std), 2) AS daily_co2_kg_lower_90,
        round(total_co2_kg_mean + 1.645 * total_co2_kg_std, 2) AS daily_co2_kg_upper_90,
        
        -- Annual CO2 projections
        round(total_co2_kg_mean * 365 / 1000, 2) AS annual_co2_tonnes_mean,
        round(total_co2_kg_std * sqrt(365) / 1000, 2) AS annual_co2_tonnes_std,
        
        -- Category breakdowns for energy
        round(home_staker_energy_kwh, 2) AS home_staker_energy_kwh_daily,
        round(professional_energy_kwh, 2) AS professional_energy_kwh_daily,
        round(cloud_energy_kwh, 2) AS cloud_energy_kwh_daily,
        round(unknown_energy_kwh, 2) AS unknown_energy_kwh_daily,
        
        -- Category breakdowns for carbon
        round(home_staker_co2_kg, 2) AS home_staker_co2_kg_daily,
        round(professional_co2_kg, 2) AS professional_co2_kg_daily,
        round(cloud_co2_kg, 2) AS cloud_co2_kg_daily,
        round(unknown_co2_kg, 2) AS unknown_co2_kg_daily,
        
        -- Category percentages
        round(100.0 * home_staker_co2_kg / NULLIF(total_co2_kg_mean, 0), 1) AS home_staker_pct,
        round(100.0 * professional_co2_kg / NULLIF(total_co2_kg_mean, 0), 1) AS professional_pct,
        round(100.0 * cloud_co2_kg / NULLIF(total_co2_kg_mean, 0), 1) AS cloud_pct,
        
        -- Node distribution
        home_staker_nodes,
        professional_nodes,
        cloud_nodes,
        unknown_nodes,
        
        -- Relative uncertainties
        round(100.0 * total_energy_kwh_std / NULLIF(total_energy_kwh_mean, 0), 1) AS energy_relative_uncertainty_pct,
        round(100.0 * total_co2_kg_std / NULLIF(total_co2_kg_mean, 0), 1) AS carbon_relative_uncertainty_pct,
        
        -- Quality metrics
        active_categories,
        max_countries_in_category AS countries_with_nodes
        
    FROM chao1_comparison
)

SELECT
    date,
    
    -- PRIMARY CARBON FOOTPRINT METRICS WITH BANDS
    daily_co2_kg_mean,
    daily_co2_kg_std,
    daily_co2_kg_lower_95,
    daily_co2_kg_upper_95,
    daily_co2_kg_lower_90,
    daily_co2_kg_upper_90,
    
    -- Annual CO2 projections with uncertainty bands
    annual_co2_tonnes_mean AS annual_co2_tonnes_projected,
    annual_co2_tonnes_std,
    round(greatest(0, annual_co2_tonnes_mean - 1.96 * annual_co2_tonnes_std), 2) AS annual_co2_tonnes_lower_95,
    round(annual_co2_tonnes_mean + 1.96 * annual_co2_tonnes_std, 2) AS annual_co2_tonnes_upper_95,
    round(greatest(0, annual_co2_tonnes_mean - 1.645 * annual_co2_tonnes_std), 2) AS annual_co2_tonnes_lower_90,
    round(annual_co2_tonnes_mean + 1.645 * annual_co2_tonnes_std, 2) AS annual_co2_tonnes_upper_90,
    
    -- PRIMARY ENERGY METRICS WITH BANDS
    daily_energy_kwh_mean AS daily_energy_kwh_total,
    daily_energy_kwh_std,
    daily_energy_kwh_lower_95,
    daily_energy_kwh_upper_95,
    daily_energy_kwh_lower_90,
    daily_energy_kwh_upper_90,
    
    -- Annual energy projections with uncertainty bands
    annual_energy_mwh_mean AS annual_energy_Mwh_projected,
    annual_energy_mwh_std,
    annual_energy_mwh_lower_95,
    annual_energy_mwh_upper_95,
    annual_energy_mwh_lower_90,
    annual_energy_mwh_upper_90,
    
    -- NETWORK CARBON INTENSITY WITH UNCERTAINTY
    network_carbon_intensity_gco2_kwh AS effective_carbon_intensity,
    network_carbon_intensity_std AS effective_carbon_intensity_std,
    round(greatest(0, network_carbon_intensity_gco2_kwh - 1.96 * network_carbon_intensity_std), 2) AS effective_carbon_intensity_lower_95,
    round(network_carbon_intensity_gco2_kwh + 1.96 * network_carbon_intensity_std, 2) AS effective_carbon_intensity_upper_95,
    
    -- NODE POPULATION WITH BOUNDS
    total_estimated_nodes AS estimated_validator_nodes,
    total_nodes_lower_95 AS validator_nodes_lower_95,
    total_nodes_upper_95 AS validator_nodes_upper_95,
    
    -- Category breakdown for energy (daily)
    home_staker_energy_kwh_daily,
    professional_energy_kwh_daily,
    cloud_energy_kwh_daily,
    unknown_energy_kwh_daily,
    
    -- Category breakdown for emissions (daily)
    home_staker_co2_kg_daily,
    professional_co2_kg_daily,
    cloud_co2_kg_daily,
    unknown_co2_kg_daily,
    
    -- Category percentages
    home_staker_pct,
    professional_pct,
    cloud_pct,
    
    -- Node distribution by category
    home_staker_nodes,
    professional_nodes,
    cloud_nodes,
    unknown_nodes,
    
    -- UNCERTAINTY METRICS
    energy_relative_uncertainty_pct,
    carbon_relative_uncertainty_pct,
    
    -- Quality metrics
    active_categories AS node_categories_active,
    countries_with_nodes,
    
    -- Comparison with Chao-1 estimates
    chao1_observed AS baseline_observed_nodes,
    chao1_estimated AS chao1_total_estimated,
    node_estimate_vs_chao1_pct,
    applied_scaling_factor,
    round(chao1_success_rate, 1) AS network_reachability_pct,
    round(chao1_coverage, 1) AS discovery_completeness_pct,
    
    -- PER-NODE METRICS WITH BOUNDS
    round(daily_co2_kg_mean / NULLIF(total_estimated_nodes, 0) * 1000, 1) AS grams_co2_per_node_daily,
    round(daily_energy_kwh_mean / NULLIF(total_estimated_nodes, 0) * 1000, 1) AS wh_per_node_daily,
    
    -- Per-node uncertainty bands
    round(greatest(0, (daily_co2_kg_mean - 1.96 * daily_co2_kg_std) / NULLIF(total_estimated_nodes, 0) * 1000), 1) AS grams_co2_per_node_lower_95,
    round((daily_co2_kg_mean + 1.96 * daily_co2_kg_std) / NULLIF(total_estimated_nodes, 0) * 1000, 1) AS grams_co2_per_node_upper_95,
    
    round(greatest(0, (daily_energy_kwh_mean - 1.96 * daily_energy_kwh_std) / NULLIF(total_estimated_nodes, 0) * 1000), 1) AS wh_per_node_lower_95,
    round((daily_energy_kwh_mean + 1.96 * daily_energy_kwh_std) / NULLIF(total_estimated_nodes, 0) * 1000, 1) AS wh_per_node_upper_95
    
FROM enhanced_statistics
ORDER BY date DESC