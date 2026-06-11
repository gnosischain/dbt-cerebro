{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_avatar_personal_token_supply', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_circles_v2_balances_daily') }}) AS as_of_date
FROM (
SELECT
    avatar,
    supply,
    wrapped,
    unwrapped,
    wrapped_pct,
    supply_demurraged,
    wrapped_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_personal_token_supply_latest') }}
) AS sub
