{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:gpay_cashback', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(month) AS quarter,
    round(sum(cashback_total_usd), 2) AS cashback_usd
FROM {{ ref('fct_execution_gpay_kpi_monthly') }}
GROUP BY quarter
ORDER BY quarter
