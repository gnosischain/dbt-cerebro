{{
    config(
        materialized='view',
        tags=["production", "consensus", "attestations"]
    )
}}

SELECT
   slot,
    attestation_index,
    aggregation_bits,
    signature,
    attestation_slot,
    committee_index,
    beacon_block_root,
    source_epoch,
    source_root,
    target_epoch,
    target_root,
    slot_timestamp
FROM 
    {{ source('consensus', 'attestations') }} FINAL
