{{
  config(
    materialized='view',
    tags=['production','celo','gpay','tier0','api:celo_gpay_total_balance','granularity:all_time']
  )
}}

-- Spendable stablecoin float held across all Celo GP card Safes (latest day),
-- mark-to-market in USD. Scope is token_class = 'STABLECOIN' (USDT/USDC in practice);
-- RWA reward holdings are the separate RewardBalance label, not included here.
SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_celo_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_celo_gpay_snapshots') }}
WHERE label = 'TotalBalance' AND window = 'All'
) AS sub
