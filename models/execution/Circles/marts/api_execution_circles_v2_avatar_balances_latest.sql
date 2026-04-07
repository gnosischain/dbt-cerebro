{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_balances_latest', 'granularity:snapshot']
    )
}}

WITH latest AS (
    SELECT max(date) AS d
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE date < today()
)

SELECT
    b.account AS avatar,
    b.token_address,
    toFloat64(b.balance_raw) / pow(10, 18) AS balance,
    toFloat64(b.demurraged_balance_raw) / pow(10, 18) AS balance_demurraged
FROM {{ ref('int_execution_circles_v2_balances_daily') }} b
CROSS JOIN latest
WHERE b.date = latest.d AND b.balance_raw > 0
