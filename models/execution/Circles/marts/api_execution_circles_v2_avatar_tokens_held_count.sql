{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_tokens_held_count', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_circles_v2_balances_daily') }}) AS as_of_date
FROM (
SELECT
    avatar,
    tokens_held_count
FROM {{ ref('fct_execution_circles_v2_avatar_tokens_held_count') }}
) AS sub
