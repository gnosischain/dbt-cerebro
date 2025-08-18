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

WITH

time_helpers AS (
    SELECT
        genesis_time_unix,
        seconds_per_slot
    FROM 
        {{ ref('stg_consensus__time_helpers') }}
)

SELECT
    date
    ,cnt AS blocks_produced
    ,CASE
        WHEN toStartOfDay(toDateTime(genesis_time_unix)) = date 
            THEN (86400 - toUnixTimestamp(toDateTime(genesis_time_unix)) % 86400) / seconds_per_slot - cnt
        ELSE 86400 / seconds_per_slot - cnt 
    END AS blocks_missed
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,COUNT(*) AS cnt
    FROM {{ ref('stg_consensus__blocks') }}
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
) t1
CROSS JOIN time_helpers t2
    