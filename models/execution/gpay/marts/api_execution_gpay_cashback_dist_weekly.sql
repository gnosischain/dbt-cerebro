{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_dist_weekly','granularity:weekly']
  )
}}

SELECT
    week AS date,
    unit,
    q05, q10, q25, q50, q75, q90, q95,
    average
FROM {{ ref('fct_execution_gpay_cashback_dist_weekly') }}
ORDER BY date, unit
