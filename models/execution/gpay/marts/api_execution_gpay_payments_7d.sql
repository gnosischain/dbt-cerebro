{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_payments','granularity:last_7d']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT value, change_pct
FROM {{ ref('fct_execution_gpay_snapshots') }}
WHERE label = 'PaymentCount' AND window = '7D'
) AS sub
