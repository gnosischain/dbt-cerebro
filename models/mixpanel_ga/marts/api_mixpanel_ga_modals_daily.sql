{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'granularity:daily']
  )
}}

SELECT * FROM {{ ref('int_mixpanel_ga_modals_daily') }}
