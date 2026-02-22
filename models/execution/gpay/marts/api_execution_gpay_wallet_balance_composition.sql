{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_wallet_balance_composition','granularity:latest']
  )
}}

WITH latest AS (
    SELECT symbol, balance_usd
    FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }}
    WHERE date = (SELECT max(date) FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }})
      AND balance_usd > 0
),

total AS (
    SELECT sum(balance_usd) AS total_usd FROM latest
),

labeled AS (
    SELECT
        CASE
            WHEN balance_usd / t.total_usd >= 0.01 THEN symbol
            ELSE 'Other'
        END AS name,
        balance_usd AS value
    FROM latest l
    CROSS JOIN total t
)

SELECT name, round(toFloat64(sum(value)), 2) AS value
FROM labeled
GROUP BY name
ORDER BY value DESC
