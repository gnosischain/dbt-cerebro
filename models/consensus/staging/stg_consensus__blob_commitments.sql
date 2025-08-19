
SELECT
   slot,
    commitment_index,
    commitment,
    slot_timestamp
FROM 
    {{ source('consensus', 'blob_commitments') }} FINAL
