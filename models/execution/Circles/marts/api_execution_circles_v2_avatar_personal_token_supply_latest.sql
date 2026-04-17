{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_personal_token_supply_latest','granularity:latest']
    )
}}

SELECT
    avatar,
    supply,
    wrapped,
    unwrapped,
    wrapped_pct,
    supply_demurraged,
    wrapped_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_personal_token_supply_latest') }}
