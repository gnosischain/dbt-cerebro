

SELECT
    slot
    proposer_index,
    total,
    attestations,
    sync_aggregate,
    proposer_slashings,
    attester_slashings,
    slot_timestamp
FROM 
    `consensus`.`rewards` FINAL