{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'granularity:weekly']
  )
}}

-- Aggregate-only (weekly cohort counts, no pseudonyms). cerebro-api exposure
-- is blanket-excluded for all models/mixpanel_ga/ via dbt_project.yml.
-- Weekly signup cohort -> card-eligibility -> order-started -> ordered funnel.
-- Rates are ratios over the eligible denominator — do NOT sum.

SELECT
    week,
    signups,
    card_eligible,
    eligible_order_started,
    eligible_card_ordered,
    order_started_rate,
    card_ordered_rate
FROM {{ ref('fct_mixpanel_ga_card_funnel_weekly') }}
ORDER BY week
