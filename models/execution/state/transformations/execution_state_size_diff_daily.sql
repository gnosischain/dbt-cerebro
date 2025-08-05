{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date, address)',
        partition_by='toStartOfMonth(date)'
    )
}}


WITH


state_size_diff AS (
    SELECT 
        address
        ,toStartOfDay(block_timestamp) AS date 
        ,SUM(IF(to_value!='0x0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        {{ source('execution','storage_diffs') }}
    WHERE
        block_timestamp < today()
        {{ apply_monthly_incremental_filter('block_timestamp', add_and='true') }}
    GROUP BY 1, 2
)

SELECT
    *
FROM state_size_diff

        
