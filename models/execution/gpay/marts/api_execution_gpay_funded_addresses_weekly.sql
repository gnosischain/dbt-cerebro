{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_funded_addresses_weekly','granularity:weekly']
  )
}}

SELECT
    week              AS date,
    cumulative_funded AS value
FROM {{ ref('fct_execution_gpay_activity_weekly') }}
ORDER BY date
