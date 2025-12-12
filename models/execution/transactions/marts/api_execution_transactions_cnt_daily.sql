{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:transactions_count_per_type', 'granularity:daily']
    )
}}

SELECT
    date
    ,transaction_type
    ,n_txs AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
ORDER BY date, transaction_type
