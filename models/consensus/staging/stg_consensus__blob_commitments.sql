{{
    config(
        materialized='view',
        tags=["production", "consensus", "blob_commitments"]
    )
}}

SELECT
   slot,
    commitment_index,
    commitment,
    slot_timestamp
FROM 
    {{ source('consensus', 'blob_commitments') }} FINAL
