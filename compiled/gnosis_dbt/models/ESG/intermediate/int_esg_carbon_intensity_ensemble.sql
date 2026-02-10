

WITH ember_monthly AS (
    -- Monthly carbon intensity from Ember with enhanced uncertainty
    SELECT
        toDate("Date") AS month_date,
        CASE 
            WHEN "Area" = 'World' THEN 'WORLD'  -- World average as special country code
            ELSE "ISO 3 code"
        END AS country_code,
        "Value" AS carbon_intensity_gco2_kwh,
        COALESCE("Continent", 'World') AS continent,
        'ember' AS source,
        0.85 AS base_confidence
    FROM `dbt`.`stg_crawlers_data__ember_electricity_data`
    WHERE   
        "Unit" = 'gCO2/kWh'
        AND (
            ("ISO 3 code" IS NOT NULL AND "ISO 3 code" != '') OR 
            ("Area" = 'World')  -- Include World data
        )
        AND "Value" IS NOT NULL
        AND "Value" > 0
        
  
    
    

   AND 
    toStartOfMonth(toDate("Date")) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.month_date)), -0))
      FROM `dbt`.`int_esg_carbon_intensity_ensemble` AS x1
      WHERE 1=1 
    )
    AND toDate("Date") >= (
      SELECT addDays(max(toDate(x2.month_date)), -0)
      FROM `dbt`.`int_esg_carbon_intensity_ensemble` AS x2
      WHERE 1=1 
    )
  

),

-- Enhanced uncertainty modeling for monthly data
uncertainty_enhanced AS (
    SELECT
        month_date,
        country_code,
        continent,
        carbon_intensity_gco2_kwh AS base_ci,
        
        -- Calculate temporal uncertainty based on grid characteristics
        -- Higher uncertainty for countries with more variable generation
        carbon_intensity_gco2_kwh * (
            CASE 
                -- High renewable countries have more temporal variation
                WHEN carbon_intensity_gco2_kwh < 100 THEN 0.25  -- Low carbon grids (solar/wind heavy)
                WHEN carbon_intensity_gco2_kwh < 300 THEN 0.20  -- Medium carbon grids  
                WHEN carbon_intensity_gco2_kwh < 600 THEN 0.15  -- High carbon grids (more stable)
                ELSE 0.12  -- Very high carbon grids (coal/gas baseload)
            END
        ) AS temporal_uncertainty,
        
        -- Data quality uncertainty (monthly averages hide daily/hourly variation)
        carbon_intensity_gco2_kwh * 0.10 AS measurement_uncertainty,
        
        -- Continent-based seasonal adjustment factors
        CASE continent
            -- Europe & Asia (mostly Northern Hemisphere, heating-dominant)
            WHEN 'Europe' THEN
                CASE 
                    WHEN month(month_date) IN (12, 1, 2) THEN 1.18  -- Winter heating peak
                    WHEN month(month_date) IN (6, 7, 8) THEN 0.92   -- Summer low + solar
                    WHEN month(month_date) IN (3, 4, 11) THEN 1.08  -- Shoulder seasons
                    ELSE 1.0
                END
                
            -- Asia (mix of climates, but mostly Northern Hemisphere)
            WHEN 'Asia' THEN
                CASE 
                    WHEN month(month_date) IN (12, 1, 2) THEN 1.12  -- Winter (heating + industrial)
                    WHEN month(month_date) IN (6, 7, 8) THEN 1.08   -- Summer (cooling demand)
                    WHEN month(month_date) IN (4, 5, 9, 10) THEN 1.05  -- Shoulder seasons
                    ELSE 1.0
                END
                
            -- North America (heating-dominant north, cooling-dominant south)
            WHEN 'North America' THEN
                CASE 
                    WHEN month(month_date) IN (12, 1, 2) THEN 1.15  -- Winter heating
                    WHEN month(month_date) IN (6, 7, 8) THEN 1.12   -- Summer cooling
                    WHEN month(month_date) IN (4, 5, 9, 10) THEN 1.03  -- Shoulder seasons
                    ELSE 1.0
                END
                
            -- Oceania (Southern Hemisphere - reversed seasons)
            WHEN 'Oceania' THEN
                CASE 
                    WHEN month(month_date) IN (6, 7, 8) THEN 1.15   -- Southern winter
                    WHEN month(month_date) IN (12, 1, 2) THEN 0.95  -- Southern summer
                    WHEN month(month_date) IN (3, 4, 9, 10) THEN 1.05  -- Shoulder seasons
                    ELSE 1.0
                END
                
            -- South America (Southern Hemisphere + tropical)
            WHEN 'South America' THEN
                CASE 
                    WHEN month(month_date) IN (6, 7, 8) THEN 1.10   -- Southern winter (milder)
                    WHEN month(month_date) IN (12, 1, 2) THEN 0.98  -- Southern summer
                    ELSE 1.0
                END
                
            -- Africa (mix of Northern/Southern + tropical, minimal variation)
            WHEN 'Africa' THEN
                CASE 
                    WHEN month(month_date) IN (6, 7, 8) THEN 1.05   -- Slight dry season effect
                    WHEN month(month_date) IN (12, 1, 2) THEN 0.98  -- Wet season
                    ELSE 1.0
                END
                
            -- World/Default (minimal adjustment)
            ELSE 1.0
        END AS seasonal_factor,
        
        source,
        base_confidence
        
    FROM ember_monthly
),

-- Final aggregation with confidence intervals
final_estimates AS (
    SELECT
        month_date,
        country_code,
        
        -- Point estimates with seasonal adjustment
        round(base_ci * seasonal_factor, 2) AS carbon_intensity_mean,
        
        -- Combined uncertainty (temporal + measurement)
        round(sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS carbon_intensity_std,
        
        -- Confidence intervals for Monte Carlo sampling
        round(greatest(0, base_ci * seasonal_factor - 1.96 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2))), 2) AS ci_lower_95,
        round(base_ci * seasonal_factor + 1.96 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS ci_upper_95,
        
        round(greatest(0, base_ci * seasonal_factor - 1.645 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2))), 2) AS ci_lower_90,
        round(base_ci * seasonal_factor + 1.645 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS ci_upper_90,
        
        -- Coefficient of variation for uncertainty assessment
        round(sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / (base_ci * seasonal_factor), 3) AS coefficient_of_variation,
        
        -- Raw values for diagnostics
        round(base_ci, 2) AS base_carbon_intensity,
        round(temporal_uncertainty, 2) AS temporal_std,
        round(measurement_uncertainty, 2) AS measurement_std,
        round(seasonal_factor, 3) AS seasonal_adjustment,
        continent,
        
        -- Data quality indicators
        arrayPushFront([], source) AS sources_used,
        base_confidence AS confidence_score,
        1 AS n_sources,
        
        -- Uncertainty category for diagnostics
        CASE 
            WHEN sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / (base_ci * seasonal_factor) < 0.15 THEN 'Low'
            WHEN sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / (base_ci * seasonal_factor) < 0.25 THEN 'Medium' 
            ELSE 'High'
        END AS uncertainty_category
        
    FROM uncertainty_enhanced
)

SELECT
    month_date,
    country_code,
    
    -- Core metrics for carbon footprint calculation
    carbon_intensity_mean,
    carbon_intensity_std,
    ci_lower_95,
    ci_upper_95,
    ci_lower_90,
    ci_upper_90,
    
    -- Uncertainty analysis
    coefficient_of_variation,
    uncertainty_category,
    
    -- Component breakdown for diagnostics
    base_carbon_intensity,
    temporal_std,
    measurement_std,
    seasonal_adjustment,
    
    -- Data provenance and quality
    sources_used,
    confidence_score,
    n_sources,
    
    -- Metadata
    now() AS calculated_at
    
FROM final_estimates