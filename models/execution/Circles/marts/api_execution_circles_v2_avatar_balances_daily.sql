{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_balances_daily', 'granularity:daily']
    )
}}

SELECT
    avatar,
    date,
    token_address,
    balance,
    balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }}
WHERE date < today()
