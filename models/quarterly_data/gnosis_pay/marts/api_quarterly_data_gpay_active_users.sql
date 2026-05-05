{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gpay_active_users', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    max(mau) AS peak_monthly_active_users
FROM {{ ref('fct_execution_gpay_kpi_monthly') }}
GROUP BY quarter
ORDER BY quarter
