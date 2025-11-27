{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'transactions', 'tier0', 'api: cnt_by_transaction_type_total']
    )
}}

SELECT
    transaction_type
    ,SUM(n_txs) AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY transaction_type
ORDER BY transaction_type
