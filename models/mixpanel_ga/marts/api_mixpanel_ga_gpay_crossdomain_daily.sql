{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'gpay', 'tier3', 'granularity:daily']
  )
}}

SELECT * FROM {{ ref('fct_mixpanel_ga_gpay_crossdomain_daily') }}
