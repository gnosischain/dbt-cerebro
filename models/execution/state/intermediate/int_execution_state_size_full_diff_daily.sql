{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['production','execution','state','size']
    )
}}


WITH


state_size_diff AS (
    SELECT 
        toStartOfDay(block_timestamp) AS date 
        ,SUM(IF(to_value!='0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        {{ ref('stg_execution__storage_diffs') }}
    {{ apply_monthly_incremental_filter('block_timestamp','date') }}
    GROUP BY 1
)

SELECT
    *
FROM state_size_diff

        
