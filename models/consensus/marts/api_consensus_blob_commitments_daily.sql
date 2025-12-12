{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:blob_commitments', 'granularity:daily']
    )
}}

SELECT
    date
    ,total_blob_commitments AS value
FROM {{ ref('int_consensus_blocks_daily') }}
ORDER BY date
