{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_active_trusts', 'granularity:daily']
    )
}}

SELECT
    date,
    new_trusts,
    revoked_trusts,
    active_trusts
FROM {{ ref('fct_execution_circles_v2_active_trusts_daily') }}
WHERE date < today()
ORDER BY date DESC
