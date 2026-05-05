{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gpay_payments', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    sum(total_payment_count) AS payments
FROM {{ ref('fct_execution_gpay_kpi_monthly') }}
GROUP BY quarter
ORDER BY quarter
