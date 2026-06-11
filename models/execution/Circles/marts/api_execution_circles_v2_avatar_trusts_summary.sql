{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_trusts_summary', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(day)) FROM {{ ref('fct_execution_circles_v2_avatar_trusts_daily') }}) AS as_of_date
FROM (
SELECT
    avatar,
    trusts_given_count,
    trusts_received_count
FROM {{ ref('fct_execution_circles_v2_avatar_trusts_summary') }}
) AS sub
