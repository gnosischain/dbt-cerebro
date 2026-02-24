{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_funded_addresses_monthly','granularity:monthly']
  )
}}

SELECT
    month             AS date,
    cumulative_funded AS value
FROM {{ ref('fct_execution_gpay_activity_monthly') }}
ORDER BY date
