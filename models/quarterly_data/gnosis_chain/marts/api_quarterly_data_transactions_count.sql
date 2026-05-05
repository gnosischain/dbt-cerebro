{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:transactions_count', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    sum(n_txs) AS transactions
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY quarter
ORDER BY quarter
