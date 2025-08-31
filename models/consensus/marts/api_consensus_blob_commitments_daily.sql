{{
    config(
        materialized='view',
        tags=["production", "consensus", "blob_commitments"]
    )
}}

SELECT
    date
    ,cnt AS value
FROM {{ ref('int_consensus_blob_commitments_daily') }}
ORDER BY date
