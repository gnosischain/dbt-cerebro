{{
    config(
        materialized='view',
        tags=['production','crawlers_data']
    )
}}

-- Ember monthly electricity data ("generation" release): one row per
-- area x month x electricity source. The raw table is a full reload twice a month
-- into a ReplacingMergeTree(ingested_at); FINAL keeps the newest copy of each key.
-- iso3_code is '' for Ember's aggregate regions (World, EU, G20, ...).

SELECT
    "Area"                                      AS area,
    "ISO 3 code"                                AS iso3_code,
    "Date"                                      AS month_date,
    "Area type"                                 AS area_type,
    "Electricity source"                        AS electricity_source,
    "Is aggregated source"                      AS is_aggregated_source,
    "Generation (TWh)"                          AS generation_twh,
    "Generation YoY change (TWh)"               AS generation_yoy_change_twh,
    "Generation YoY change (%)"                 AS generation_yoy_change_pct,
    "Share of generation (%)"                   AS share_of_generation_pct,
    "Share of generation YoY change (% points)" AS share_of_generation_yoy_change_pp,
    "Emissions (MtCO2e)"                        AS emissions_mtco2e,
    "Emissions YoY change (MtCO2e)"             AS emissions_yoy_change_mtco2e,
    "Emissions YoY change (%)"                  AS emissions_yoy_change_pct,
    "Share of emissions (%)"                    AS share_of_emissions_pct,
    "Emissions intensity (gCO2e/kWh)"           AS emissions_intensity_gco2e_kwh,
    "Continent"                                 AS continent,
    "Ember region"                              AS ember_region,
    "EU member"                                 AS is_eu_member,
    "OECD member"                               AS is_oecd_member,
    "G20 member"                                AS is_g20_member,
    "G7 member"                                 AS is_g7_member,
    "ASEAN member"                              AS is_asean_member,
    ingested_at
FROM {{ source('crawlers_data', 'ember_generation_monthly') }} FINAL
