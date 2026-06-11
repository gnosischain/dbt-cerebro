{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_total_payments','granularity:all_time']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'PaymentCount' AND window = 'All'
) AS sub
