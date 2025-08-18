{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, graffiti)',
        unique_key='(date, graffiti)',
        partition_by='toStartOfMonth(date)'
    )
}}



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,graffiti
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__blocks') }}
WHERE
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY 1, 2

    