{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_kpi', 'granularity:monthly']
  )
}}

SELECT *
FROM {{ ref('fct_celo_gpay_kpi_monthly') }}
