{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga']
  )
}}

-- Aggregate-only (k-anonymized, no pseudonyms). cerebro-api exposure is
-- blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml.

SELECT
    cohort_week,
    weeks_since,
    utm_campaign,
    utm_source,
    utm_medium,
    cohort_size,
    retained_accounts,
    retention_pct
FROM {{ ref('fct_mixpanel_ga_gpay_campaign_retention_weekly') }}
ORDER BY cohort_week, weeks_since, utm_campaign
