{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'esg', 'carbon_footprint']
    )
}}

{#
  Extends fct_esg_carbon_footprint_uncertainty with estimated values for periods
  where Ember carbon intensity data is missing (Jan 2026+).

  Strategy:
  - For dates covered by the fact table → use production values directly
  - For dates after the fact table's last date → recompute from:
      * Node distribution (still flowing from P2P crawlers)
      * Client efficiency (still flowing from P2P crawlers)
      * Forward-filled carbon intensity (last known Ember values)
  - Marks estimated rows with is_estimated = true
#}

WITH existing_data AS (
    SELECT
        date,
        annual_co2_tonnes_projected,
        annual_energy_Mwh_projected,
        false AS is_estimated
    FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
),

last_existing_date AS (
    SELECT max(date) AS max_date FROM existing_data
),

node_distribution AS (
    SELECT
        nd.date,
        nd.node_category,
        nd.country_code,
        nd.country_code_alpha3,
        nd.estimated_total_nodes,
        nd.nodes_lower_95,
        nd.nodes_upper_95
    FROM {{ ref('int_esg_node_geographic_distribution') }} nd
    CROSS JOIN last_existing_date led
    WHERE nd.date > led.max_date
      AND nd.date < today()
),

client_efficiency_by_category AS (
    SELECT
        ncd.date,
        ncd.node_category,
        SUM(ncd.category_client_percentage / 100.0 * ncd.client_efficiency_factor) AS avg_client_efficiency,
        COUNT(DISTINCT ncd.client_type) AS client_diversity
    FROM {{ ref('int_esg_node_client_distribution') }} ncd
    CROSS JOIN last_existing_date led
    WHERE ncd.date > led.max_date
      AND ncd.date < today()
    GROUP BY ncd.date, ncd.node_category
),

power_per_node AS (
    SELECT
        nd.date,
        nd.node_category,
        nd.country_code_alpha3,
        nd.estimated_total_nodes,

        CASE nd.node_category
            WHEN 'home_staker' THEN 22.0
            WHEN 'professional_operator' THEN 48.0
            WHEN 'cloud_hosted' THEN 155.0
            ELSE 50.0
        END AS base_power_watts,

        CASE nd.node_category
            WHEN 'home_staker' THEN 1.0
            WHEN 'professional_operator' THEN 1.58
            WHEN 'cloud_hosted' THEN 1.15
            ELSE 1.1
        END AS pue_factor,

        COALESCE(ce.avg_client_efficiency, 1.0) AS client_efficiency,
        CASE
            WHEN ce.client_diversity > 0 THEN 0.95 + 0.05 * least(4, ce.client_diversity) / 4.0
            ELSE 1.0
        END AS diversity_bonus

    FROM node_distribution nd
    LEFT JOIN client_efficiency_by_category ce
        ON nd.date = ce.date AND nd.node_category = ce.node_category
),

energy_and_co2 AS (
    SELECT
        p.date,
        p.node_category,
        p.country_code_alpha3,

        p.estimated_total_nodes
            * p.base_power_watts
            * p.client_efficiency
            * p.diversity_bonus
            * 24.0 / 1000.0 AS daily_energy_kwh,

        p.estimated_total_nodes
            * p.base_power_watts
            * p.client_efficiency
            * p.diversity_bonus
            * 24.0 / 1000.0
            * p.pue_factor
            * CASE
                WHEN ci_country.month_date > toDate('1970-01-02') THEN ci_country.carbon_intensity_mean
                WHEN ci_world.month_date > toDate('1970-01-02') THEN ci_world.carbon_intensity_mean
                ELSE 450.0
              END / 1000.0 AS daily_co2_kg

    FROM power_per_node p
    LEFT JOIN {{ ref('int_quarterly_esg_carbon_intensity_with_fallback') }} ci_country
        ON ci_country.country_code = p.country_code_alpha3
        AND ci_country.month_date = toStartOfMonth(p.date)
        AND p.country_code_alpha3 IS NOT NULL
        AND p.country_code_alpha3 != ''
    LEFT JOIN {{ ref('int_quarterly_esg_carbon_intensity_with_fallback') }} ci_world
        ON ci_world.country_code = 'WORLD'
        AND ci_world.month_date = toStartOfMonth(p.date)
),

estimated_daily_totals AS (
    SELECT
        date,
        SUM(daily_energy_kwh) * 365.0 / 1000.0 AS annual_energy_Mwh_projected,
        SUM(daily_co2_kg) * 365.0 / 1000.0 AS annual_co2_tonnes_projected,
        true AS is_estimated
    FROM energy_and_co2
    GROUP BY date
)

SELECT date, annual_co2_tonnes_projected, annual_energy_Mwh_projected, is_estimated
FROM existing_data

UNION ALL

SELECT date, annual_co2_tonnes_projected, annual_energy_Mwh_projected, is_estimated
FROM estimated_daily_totals
