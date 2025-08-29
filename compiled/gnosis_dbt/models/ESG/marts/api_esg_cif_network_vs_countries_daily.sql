WITH network_daily_cif AS (
    -- Get network's daily effective carbon intensity
    SELECT
        date,
        effective_carbon_intensity AS carbon_intensity,
        'GNOSIS' AS entity_code
    FROM `dbt`.`fct_esg_carbon_footprint_uncertainty`
    WHERE effective_carbon_intensity IS NOT NULL
        AND effective_carbon_intensity > 0
),

country_monthly_cif AS (
    -- Get ALL monthly country carbon intensities (not just latest)
    SELECT
        ci.country_code,
        ci.carbon_intensity_mean AS carbon_intensity,
        ci.month_date
    FROM `dbt`.`int_esg_carbon_intensity_ensemble` ci
    WHERE ci.country_code != 'WORLD'
        AND ci.carbon_intensity_mean IS NOT NULL
        AND ci.carbon_intensity_mean > 0
        AND ci.country_code IN (
            'USA',  -- United States
            'DEU',  -- Germany  
            'CHN',  -- China
            'FRA',  -- France
            'SWE',  -- Sweden (clean)
            'AUS',  -- Australia
            'BRA',  -- Brazil
            'ISL'   -- Iceland (very clean)
        )
),

country_timeseries AS (
    -- Join countries to dates using the correct month's CIF
    SELECT
        nd.date,
        cm.carbon_intensity,
        cm.country_code AS entity_code
    FROM network_daily_cif nd
    JOIN country_monthly_cif cm 
        ON cm.month_date = toStartOfMonth(nd.date)  -- Match date to its month
),

-- Combine network and country data
combined_data AS (
    SELECT * FROM network_daily_cif
    UNION ALL
    SELECT * FROM country_timeseries
),

-- Add comparison metrics
with_comparisons AS (
    SELECT
        cd.*,
        nd.carbon_intensity AS network_cif
    FROM combined_data cd
    LEFT JOIN network_daily_cif nd ON cd.date = nd.date
)

SELECT
    date,
    entity_code,
    round(carbon_intensity, 1) AS carbon_intensity_gco2_kwh
FROM with_comparisons
ORDER BY date, entity_code, carbon_intensity_gco2_kwh DESC