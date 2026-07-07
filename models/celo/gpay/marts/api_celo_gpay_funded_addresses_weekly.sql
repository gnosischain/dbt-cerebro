{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funded_addresses', 'granularity:weekly']
  )
}}

SELECT
    week              AS date,
    cumulative_funded AS value
FROM {{ ref('fct_celo_gpay_activity_weekly') }}
ORDER BY date
