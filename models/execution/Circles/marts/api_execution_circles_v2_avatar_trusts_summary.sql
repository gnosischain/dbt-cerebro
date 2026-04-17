{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_trusts_summary', 'granularity:latest']
    )
}}

SELECT
    avatar,
    trusts_given_count,
    trusts_received_count
FROM {{ ref('fct_execution_circles_v2_avatar_trusts_summary') }}
