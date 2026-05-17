{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT * FROM {{ ref('fct_mixpanel_ga_funnel_daily') }}
