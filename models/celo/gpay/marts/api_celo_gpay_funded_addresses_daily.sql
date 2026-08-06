{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funded_addresses', 'granularity:daily']
  )
}}

-- Cards that have ever RECEIVED money, cumulative. RESTATED 2026-08-05: this served
-- a first-PAYMENT count until then, i.e. activation under the funded label, reading
-- 654 against a true 1075. The old series moved to
-- api_celo_gpay_activated_addresses_daily rather than being dropped.
SELECT
    date              AS date,
    cumulative_funded AS value
FROM {{ ref('fct_celo_gpay_activity_daily') }}
ORDER BY date
