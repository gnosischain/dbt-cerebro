{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_gno_total_balance','granularity:all_time']
  )
}}

SELECT round(toFloat64(balance), 2) AS value
FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }}
WHERE symbol = 'GNO'
ORDER BY date DESC
LIMIT 1
