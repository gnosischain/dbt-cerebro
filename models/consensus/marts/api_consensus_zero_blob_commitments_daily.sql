{{
    config(
        materialized='view',
        tags=["production", "consensus", "blob_commitments"]
    )
}}


SELECT
    date
    ,label
    ,value
FROM (
    SELECT date, 'with Blobs' AS label, toInt64(blocks_produced) - toInt64(blocks_with_zero_blob_commitments) AS value FROM {{ ref('int_consensus_blocks_daily') }}
    UNION ALL 
    SELECT date, 'without Blobs' AS label, toInt64(blocks_with_zero_blob_commitments) AS value FROM {{ ref('int_consensus_blocks_daily') }}
)
ORDER BY date, label