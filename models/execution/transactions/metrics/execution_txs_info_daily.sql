{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, transaction_type, success)',
        unique_key='(date, transaction_type, success)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    ) 
}}


SELECT
    toStartOfDay(block_timestamp) AS date
    ,toString(transaction_type) AS transaction_type
    ,success
    ,COUNT(*) AS n_txs
    ,SUM(COALESCE(gas_used,0)) AS gas_used
    ,CAST(AVG(COALESCE(gas_price,0)) AS Int32) AS gas_price_avg
    ,CAST(median(COALESCE(gas_price,0)) AS Int32) AS gas_price_median
FROM {{ source('execution','transactions') }}
WHERE block_timestamp < today()
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
GROUP BY 1, 2, 3


