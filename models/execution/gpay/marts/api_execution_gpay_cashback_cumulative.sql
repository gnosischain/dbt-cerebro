{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_cumulative','granularity:weekly']
  )
}}

SELECT 'native' AS unit, week AS date, volume_cumulative AS value
FROM {{ ref('fct_execution_gpay_actions_by_token_weekly') }}
WHERE action = 'Cashback'

UNION ALL

SELECT 'usd' AS unit, week AS date, volume_usd_cumulative AS value
FROM {{ ref('fct_execution_gpay_actions_by_token_weekly') }}
WHERE action = 'Cashback'

ORDER BY date
