{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_token_distribution', 'granularity:latest']
    )
}}

SELECT
    avatar,
    holder_category,
    holder_count,
    balance,
    balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_token_distribution') }}
