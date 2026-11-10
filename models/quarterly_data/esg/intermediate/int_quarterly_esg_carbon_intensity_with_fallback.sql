{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'esg', 'carbon_intensity']
    )
}}

-- Monthly carbon intensity per country for the quarterly ESG reports: the months Ember
-- has published, as int_esg_carbon_intensity_ensemble holds them.
-- Until 2026-11 this view forward-filled each country's last known month for up to 11
-- months, flagged is_estimated = true. The ESG chain now waits for Ember's publication
-- instead of copying an older month forward, so the flag was dropped. The view name
-- stays because the semantic layer reads this view.

SELECT
    month_date,
    country_code,
    carbon_intensity_mean,
    carbon_intensity_std
FROM {{ ref('int_esg_carbon_intensity_ensemble') }}
