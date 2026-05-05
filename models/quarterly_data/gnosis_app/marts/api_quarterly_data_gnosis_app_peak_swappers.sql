{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gnosis_app_peak_swappers', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    max(n_swappers) AS peak_daily_swappers
FROM {{ ref('fct_execution_gnosis_app_swaps_daily') }}
GROUP BY quarter
ORDER BY quarter
