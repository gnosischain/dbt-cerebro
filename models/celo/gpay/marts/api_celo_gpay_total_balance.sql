{{
  config(
    materialized='view',
    tags=['production','celo','gpay','tier0','api:celo_gpay_total_balance','granularity:all_time']
  )
}}

-- Net-flow USDC+USDT float held across all Celo GP card Safes (latest day).
SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_celo_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_celo_gpay_snapshots') }}
WHERE label = 'TotalBalance' AND window = 'All'
) AS sub
