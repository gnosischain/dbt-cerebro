

SELECT
   slot,
    commitment_index,
    commitment,
    slot_timestamp
FROM 
    `consensus`.`blob_commitments` FINAL