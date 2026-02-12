{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, symbol)',
    tags=['production','execution','gpay']
  )
}}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('stg_gpay__wallets') }}
)

SELECT
    b.date,
    b.symbol,
    sum(b.balance)                        AS balance,
    round(toFloat64(sum(b.balance_usd)), 2) AS balance_usd
FROM {{ ref('int_execution_tokens_balances_daily') }} b
WHERE b.address IN (SELECT address FROM gpay_wallets)
  AND b.date >= '2023-06-01'
  AND b.date < today()
GROUP BY b.date, b.symbol
ORDER BY b.date, b.symbol
