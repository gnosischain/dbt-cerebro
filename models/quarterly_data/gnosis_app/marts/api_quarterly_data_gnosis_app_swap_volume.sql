{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gnosis_app_swap_volume', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    round(sum(volume_usd_filled), 2) AS volume_usd
FROM {{ ref('fct_execution_gnosis_app_swaps_monthly') }}
GROUP BY quarter
ORDER BY quarter
