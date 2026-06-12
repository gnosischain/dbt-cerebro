{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_token_distribution', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_circles_v2_balances_daily') }}) AS as_of_date
FROM (
SELECT
    avatar,
    holder_category,
    holder_count,
    balance,
    balance_demurraged
FROM {{ ref('fct_execution_circles_v2_avatar_token_distribution') }}
) AS sub
