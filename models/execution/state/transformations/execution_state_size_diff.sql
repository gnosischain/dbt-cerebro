{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp)',
        unique_key='(block_timestamp, address)',
        partition_by='toStartOfMonth(block_timestamp)'
    )
}}


WITH


state_size_diff AS (
    SELECT 
        address
        ,block_timestamp 
        ,SUM(IF(to_value!='0x0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        {{ source('execution','storage_diffs') }}
    {{ apply_monthly_incremental_filter('block_timestamp') }}
    GROUP BY 1, 2
)

SELECT
    *
FROM state_size_diff
WHERE block_timestamp < today()


        
