{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_weekly','granularity:weekly']
  )
}}

SELECT 'native' AS unit, week AS date, volume AS value
FROM {{ ref('fct_execution_gpay_actions_by_token_weekly') }}
WHERE action = 'Cashback'

UNION ALL

SELECT 'usd' AS unit, week AS date, volume_usd AS value
FROM  {{ ref('fct_execution_gpay_actions_by_token_weekly') }}
WHERE action = 'Cashback'

ORDER BY date