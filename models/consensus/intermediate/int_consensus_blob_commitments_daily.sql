{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "blob_commitments"]
    )
}}



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__blob_commitments') }}
WHERE
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY 1
