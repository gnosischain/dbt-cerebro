{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'esg', 'carbon_footprint']
    )
}}

{#
  Daily annualised CO2 and energy for the quarterly ESG reports, read from
  fct_esg_carbon_footprint_uncertainty.

  Until 2026-11 this view extended the fact table past its last day with estimates
  built from forward-filled carbon intensity, flagged is_estimated = true. The ESG chain
  now waits for Ember's publication instead: the fact table ends on the last day of the
  latest month for which Ember has published the World intensity (the gate in
  int_esg_dynamic_power_consumption), and this view returns those days only. Nothing is
  estimated any more, so is_estimated was dropped here and in the two quarterly API
  marts; the view name stays because the semantic layer reads it.
#}

SELECT
    date,
    annual_co2_tonnes_projected,
    annual_energy_Mwh_projected
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
