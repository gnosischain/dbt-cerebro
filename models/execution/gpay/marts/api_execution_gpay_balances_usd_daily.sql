{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_balances_usd_daily','granularity:daily']
  )
}}

SELECT
    date,
    symbol      AS label,
    balance_usd AS value
FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }}
WHERE symbol IN ('EURe', 'GBPe', 'USDC.e', 'GNO')
ORDER BY date, label
