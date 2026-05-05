{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gnosis_app_swaps_filled', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    sum(n_swaps_filled) AS swaps_filled
FROM {{ ref('fct_execution_gnosis_app_swaps_monthly') }}
GROUP BY quarter
ORDER BY quarter
