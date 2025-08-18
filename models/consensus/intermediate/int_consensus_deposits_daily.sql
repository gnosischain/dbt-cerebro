{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)'
    )
}}


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(amount/POWER(10,9)) AS total_amount
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__deposits') }}
WHERE
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY 1
