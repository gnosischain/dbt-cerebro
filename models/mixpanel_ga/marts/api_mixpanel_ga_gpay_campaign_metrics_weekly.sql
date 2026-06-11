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
    utm_campaign,
    utm_source,
    utm_medium,
    active_accounts,
    payments,
    payment_volume_usd,
    cashback_usd,
    avg_payments_per_account
FROM {{ ref('fct_mixpanel_ga_gpay_campaign_metrics_weekly') }}
ORDER BY week, utm_campaign
