{{
    config(
        materialized='view',
        tags=["production", "consensus", "rewards"]
    )
}}

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
    {{ source('consensus', 'rewards') }} FINAL
