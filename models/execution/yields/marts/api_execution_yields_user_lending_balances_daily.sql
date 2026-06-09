{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_user_lending_balances', 'granularity:daily', 'tier1']
    )
}}

SELECT
    date,
    user_address,
    reserve_address,
    symbol,
    round(balance, 6)      AS balance,
    round(balance_usd, 2)  AS balance_usd
FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
