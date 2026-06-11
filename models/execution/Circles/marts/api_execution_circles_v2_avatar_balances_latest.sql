{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_avatar_balances', 'granularity:snapshot']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }}) AS as_of_date
FROM (
SELECT
    avatar,
    token_address,
    is_wrapped,
    balance,
    balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_balances_latest') }}
) AS sub
