{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'tier1', 'api:yields_lending', 'metric:tvl_by_token', 'granularity:snapshot']
    )
}}

WITH latest_date AS (
    SELECT max(date) AS max_date
    FROM {{ ref('int_execution_yields_aave_user_balances_daily') }}
    WHERE date < today()
      AND balance_usd > 0
)

SELECT
    b.symbol AS token,
    sum(b.balance_usd) AS value
FROM {{ ref('int_execution_yields_aave_user_balances_daily') }} b
CROSS JOIN latest_date d
WHERE b.date = d.max_date
  AND b.balance_usd > 0
GROUP BY b.symbol
ORDER BY value DESC
