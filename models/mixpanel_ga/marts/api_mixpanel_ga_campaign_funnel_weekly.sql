{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga']
  )
}}

-- Aggregate-only (k-anonymized, no pseudonyms). cerebro-api exposure is
-- blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml.

SELECT
    week,
    step,
    step_order,
    utm_campaign,
    utm_source,
    utm_medium,
    new_users
FROM {{ ref('fct_mixpanel_ga_campaign_funnel_weekly') }}
ORDER BY week, step_order, utm_campaign
