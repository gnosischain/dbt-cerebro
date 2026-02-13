{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_weekly','granularity:weekly']
  )
}}

SELECT 'native' AS unit, week AS date, amount_gno AS value
FROM {{ ref('fct_execution_gpay_cashback_weekly') }}

UNION ALL

SELECT 'usd' AS unit, week AS date, amount_usd AS value
FROM {{ ref('fct_execution_gpay_cashback_weekly') }}

ORDER BY date
