{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funded_addresses', 'granularity:daily']
  )
}}

SELECT
    date              AS date,
    cumulative_funded AS value
FROM {{ ref('fct_celo_gpay_activity_daily') }}
ORDER BY date
