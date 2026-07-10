{{
  config(
    materialized='view',
    tags=['production','celo','gpay','tier0','api:celo_gpay_total_volume','granularity:all_time']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_celo_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_celo_gpay_snapshots') }}
WHERE label = 'PaymentVolume' AND window = 'All'
) AS sub
