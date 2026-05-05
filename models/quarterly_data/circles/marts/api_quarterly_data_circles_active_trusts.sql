{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:circles_active_trusts', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(active_trusts, date) AS active_trusts
FROM {{ ref('fct_execution_circles_v2_active_trusts_daily') }}
WHERE date < today()
GROUP BY quarter
ORDER BY quarter
