{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'transactions', 'tier1', 'api: cnt_by_transaction_type_d']
    )
}}

SELECT
    date
    ,transaction_type
    ,n_txs AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
ORDER BY date, transaction_type
