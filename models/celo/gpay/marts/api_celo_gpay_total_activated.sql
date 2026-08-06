{{
  config(
    materialized='view',
    tags=['production','celo','gpay','tier0','api:celo_gpay_total_activated','granularity:all_time']
  )
}}

-- All-time distinct cards that have ever SPENT. This is the value
-- api_celo_gpay_total_funded served until 2026-08-05, when that tile was corrected to
-- mean actually-funded; kept here under its true name. Strictly <= total_funded.
SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_celo_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_celo_gpay_snapshots') }}
WHERE label = 'PaymentUsers' AND window = 'All'
) AS sub
