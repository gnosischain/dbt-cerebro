{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_cashback_total','granularity:all_time']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT 'native' AS unit, value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackGNO' AND window = 'All'

UNION ALL

SELECT 'usd' AS unit, value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'CashbackVolume' AND window = 'All'
) AS sub
