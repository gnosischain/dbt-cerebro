{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_funded_addresses_daily','granularity:daily']
  )
}}

SELECT
    date              AS date,
    cumulative_funded AS value
FROM {{ ref('fct_execution_gpay_activity_daily') }}
ORDER BY date
