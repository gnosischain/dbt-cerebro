{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_weekly_gno','granularity:weekly']
  )
}}

SELECT
    week       AS date,
    amount_gno AS value
FROM {{ ref('fct_execution_gpay_cashback_weekly') }}
ORDER BY date
