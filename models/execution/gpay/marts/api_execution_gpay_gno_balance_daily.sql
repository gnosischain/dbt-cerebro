{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_gno_balance_daily','granularity:daily']
  )
}}

SELECT
    date,
    balance AS value
FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }}
WHERE symbol = 'GNO'
ORDER BY date
