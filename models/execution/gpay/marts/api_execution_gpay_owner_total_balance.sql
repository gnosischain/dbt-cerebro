{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_owner_total_balance','granularity:all_time']
  )
}}

SELECT
    round(toFloat64(sum(balance_usd)), 2) AS value
FROM {{ ref('fct_execution_gpay_owner_balances_by_token_daily') }}
WHERE date = (SELECT max(date) FROM {{ ref('fct_execution_gpay_owner_balances_by_token_daily') }})
  AND symbol IN ('EURe', 'GBPe', 'USDC.e')
