{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, status)',
        unique_key='(date, status)',
        partition_by='toStartOfMonth(date)',
    ) 
}}


SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,status
    ,COUNT(*) AS cnt
FROM {{ source('consensus', 'validators') }}
WHERE
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY 1, 2


