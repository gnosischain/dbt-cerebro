{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp)',
        unique_key='(block_timestamp, extra_data)',
        partition_by='toStartOfMonth(block_timestamp)'
    )
}}


WITH


blocks_extra_data AS (
    SELECT 
        block_timestamp
        ,extra_data
    FROM 
        {{ source('execution','blocks') }}
    WHERE 
        block_timestamp > '1970-01-01' -- remove genesis
    {{ apply_monthly_incremental_filter('block_timestamp',add_and='true') }}
)

SELECT
    *
FROM blocks_extra_data
WHERE block_timestamp < today()

        
