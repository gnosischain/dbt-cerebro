{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, version)',
        unique_key='(date, client, version)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

blocks_clients AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,{{ decode_hex_split('extra_data') }} AS decoded_extra_data
        ,COUNT(*) AS cnt
    FROM {{ ref('execution_blocks_production') }}
    {{ apply_monthly_incremental_filter('block_timestamp', 'date') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,decoded_extra_data[1] AS client
    ,IF(length(decoded_extra_data)>1, decoded_extra_data[2], '') AS version
    ,SUM(cnt) AS value
FROM blocks_clients
GROUP BY 1, 2, 3

