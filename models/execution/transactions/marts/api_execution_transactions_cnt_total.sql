{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'tier0', 'api:transactions_count_per_type', 'granularity:all_time']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_transactions_info_daily') }}) AS as_of_date
FROM (
SELECT
    transaction_type
    ,SUM(n_txs) AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY transaction_type
ORDER BY transaction_type
) AS sub
