{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "blocks"]
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
    ,total_blob_commitments
    ,blocks_with_zero_blob_commitments
    ,CASE
        WHEN toStartOfDay(toDateTime(genesis_time_unix)) = date 
            THEN CAST((86400 - toUnixTimestamp(toDateTime(genesis_time_unix)) % 86400) / seconds_per_slot - cnt AS UInt64)
        ELSE CAST(86400 / seconds_per_slot - cnt AS UInt64)
    END AS blocks_missed
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,COUNT(*) AS cnt
        ,SUM(blob_kzg_commitments_count) AS total_blob_commitments
        ,SUM(IF(blob_kzg_commitments_count = 0, 1, 0)) AS blocks_with_zero_blob_commitments
    FROM {{ ref('stg_consensus__blocks') }}
    WHERE
        slot_timestamp < today()
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
    GROUP BY 1
) t1
CROSS JOIN time_helpers t2
    