{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga']
  )
}}

-- Aggregate-only (k-anonymized, no pseudonyms). cerebro-api exposure is
-- blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml.
-- Per first-touch campaign: signups, Circles inviters (earned-reward and
-- full-invite-graph scopes), inviter rates and invites per inviter.
-- Snapshot (state-based) — campaign views are only meaningful for the
-- Mixpanel era (2025-10+).

SELECT
    utm_campaign,
    utm_source,
    utm_medium,
    signups,
    inviters_earned,
    inviter_pct_earned,
    invites_earned,
    invites_per_inviter_earned,
    inviters_full_graph,
    inviter_pct_full_graph,
    invites_full_graph,
    invites_per_inviter_full_graph
FROM {{ ref('fct_mixpanel_ga_campaign_referrals') }}
ORDER BY signups DESC, utm_campaign
