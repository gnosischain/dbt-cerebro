{{
    config(
        materialized='table',
        order_by='(month_date, country_code)',
        tags=['production','esg','carbon_intensity']
    )
}}

-- Monthly grid carbon intensity per country (ISO3) plus the World average
-- (country_code 'WORLD'), from Ember's monthly generation data.
-- Rebuilt in full on every run (a few thousand rows): Ember revises past months and
-- publishes countries with different lags, and a full rebuild always reflects the
-- latest release. Until 2026-11 this was insert_overwrite partitioned by YEAR with a
-- monthly incremental filter, so every run replaced the year partition with "latest
-- month onwards" and wiped the earlier months of that year (lesson ch-partition-cap).
-- carbon_intensity_mean is Ember's value for the month as published. Ember's monthly
-- value already reflects that month's generation mix, so it carries the season.
-- Until 2026-11 it was multiplied by a continent x month "seasonal factor" (Europe
-- x1.18 in winter, x0.92 in summer), which counted the season twice and, with factors
-- averaging 1.01-1.08 over a year, inflated annual CO2 (lesson
-- adjustment-on-measured-series).

WITH ember_rows AS (
    SELECT
        month_date,
        if(area = 'World', 'WORLD', iso3_code) AS country_code,
        emissions_intensity_gco2e_kwh,
        ingested_at
    FROM {{ ref('stg_crawlers_data__ember_generation_monthly') }}
    WHERE electricity_source = 'Total generation'
        AND (
            (area_type = 'Country or economy' AND iso3_code != '')
            OR area = 'World'
        )
        AND emissions_intensity_gco2e_kwh > 0
),

ember_monthly AS (
    -- One value per (month, country). The raw table never deletes a key, so if Ember
    -- renames an area the old name's rows stay: keep the most recent load's value.
    SELECT
        month_date,
        country_code,
        argMax(emissions_intensity_gco2e_kwh, ingested_at) AS carbon_intensity_gco2_kwh,
        'ember' AS source,
        0.85 AS base_confidence
    FROM ember_rows
    GROUP BY month_date, country_code
),

-- Enhanced uncertainty modeling for monthly data
uncertainty_enhanced AS (
    SELECT
        month_date,
        country_code,
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

        source,
        base_confidence

    FROM ember_monthly
),

-- Final aggregation with confidence intervals
final_estimates AS (
    SELECT
        month_date,
        country_code,

        -- Point estimate: Ember's monthly value as published (see the header)
        round(base_ci, 2) AS carbon_intensity_mean,

        -- Combined uncertainty (temporal + measurement)
        round(sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS carbon_intensity_std,

        -- Confidence intervals for Monte Carlo sampling
        round(greatest(0, base_ci - 1.96 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2))), 2) AS ci_lower_95,
        round(base_ci + 1.96 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS ci_upper_95,

        round(greatest(0, base_ci - 1.645 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2))), 2) AS ci_lower_90,
        round(base_ci + 1.645 * sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)), 2) AS ci_upper_90,

        -- Coefficient of variation for uncertainty assessment
        round(sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / base_ci, 3) AS coefficient_of_variation,

        -- Raw values for diagnostics
        round(base_ci, 2) AS base_carbon_intensity,
        round(temporal_uncertainty, 2) AS temporal_std,
        round(measurement_uncertainty, 2) AS measurement_std,

        -- Data quality indicators
        arrayPushFront([], source) AS sources_used,
        base_confidence AS confidence_score,
        1 AS n_sources,

        -- Uncertainty category for diagnostics
        CASE
            WHEN sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / base_ci < 0.15 THEN 'Low'
            WHEN sqrt(pow(temporal_uncertainty, 2) + pow(measurement_uncertainty, 2)) / base_ci < 0.25 THEN 'Medium'
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

    -- Data provenance and quality
    sources_used,
    confidence_score,
    n_sources,

    -- Metadata
    now() AS calculated_at

FROM final_estimates
