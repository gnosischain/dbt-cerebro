{{
    config(
        materialized='view',
        tags=["production", "consensus", "blob_commitments"]
    )
}}

SELECT
    date
    ,total_blob_commitments AS value
FROM {{ ref('int_consensus_blocks_daily') }}
ORDER BY date
