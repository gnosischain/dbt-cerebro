{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'gnosis_app', 'granularity:weekly']
  )
}}

-- Aggregate-only projection of fct_mixpanel_ga_gnosis_app_acquisition_weekly.
-- Campaign-level counts only — no user_pseudonym / user_id_hash — so it is
-- safe for the MCP / semantic layer. cerebro-api exposure is blanket-excluded
-- for all models/mixpanel_ga/ via dbt_project.yml (no api:* tag here).

SELECT
    week,
    conversion_kind,
    attribution_model,
    utm_campaign,
    new_accounts,
    cumulative_accounts
FROM {{ ref('fct_mixpanel_ga_gnosis_app_acquisition_weekly') }}
ORDER BY week, conversion_kind, attribution_model, utm_campaign
