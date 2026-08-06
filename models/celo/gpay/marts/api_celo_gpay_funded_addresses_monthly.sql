{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funded_addresses', 'granularity:monthly']
  )
}}

-- Cards that have ever RECEIVED money, cumulative. RESTATED 2026-08-05 — see
-- api_celo_gpay_funded_addresses_daily.
SELECT
    month             AS date,
    cumulative_funded AS value
FROM {{ ref('fct_celo_gpay_activity_monthly') }}
ORDER BY date
