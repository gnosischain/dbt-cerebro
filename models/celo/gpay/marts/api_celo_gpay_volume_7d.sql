{{
  config(
    materialized='view',
    tags=['production','celo','gpay','tier0','api:celo_gpay_volume','granularity:last_7d']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_celo_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value, change_pct
FROM {{ ref('fct_celo_gpay_snapshots') }}
WHERE label = 'PaymentVolume' AND window = '7D'
) AS sub
