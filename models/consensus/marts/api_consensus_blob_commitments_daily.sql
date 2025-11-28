{{
    config(
        materialized='view',
        tags=["production", "consensus", "blob_commitments", 'tier1', 'api: blob_commitments_d']
    )
}}

SELECT
    date
    ,total_blob_commitments AS value
FROM {{ ref('int_consensus_blocks_daily') }}
ORDER BY date
