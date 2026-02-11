{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_cumulative_usd','granularity:weekly']
  )
}}

SELECT
    week           AS date,
    cumulative_usd AS value
FROM {{ ref('fct_execution_gpay_cashback_weekly') }}
ORDER BY date
