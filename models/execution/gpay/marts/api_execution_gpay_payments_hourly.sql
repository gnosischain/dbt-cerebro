{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_payments_hourly','granularity:hourly']
  )
}}

SELECT
    hour          AS date,
    symbol        AS label,
    payment_count AS value
FROM {{ ref('fct_execution_gpay_payments_hourly') }}
ORDER BY date, label
