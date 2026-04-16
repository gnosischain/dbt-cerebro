{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_balances_latest', 'granularity:snapshot']
    )
}}

SELECT
    avatar,
    token_address,
    is_wrapped,
    balance,
    balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_balances_latest') }}
