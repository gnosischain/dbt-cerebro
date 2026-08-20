{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_activated_addresses', 'granularity:weekly']
  )
}}

-- Cards that have ever SPENT, cumulative. See api_celo_gpay_activated_addresses_daily
-- for why this series exists separately from the funded one.
SELECT
    week                 AS date,
    cumulative_activated AS value
FROM {{ ref('fct_celo_gpay_activity_weekly') }}
ORDER BY date
