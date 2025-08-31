{{
    config(
        materialized='view',
        tags=["production", "consensus", "blocks"]
    )
}}

SELECT
   slot,
    proposer_index,
    parent_root,
    state_root,
    signature,
    version,
    randao_reveal,
    graffiti,
    eth1_deposit_root,
    eth1_deposit_count,
    eth1_block_hash,
    sync_aggregate_participation,
    withdrawals_count,
    blob_kzg_commitments_count,
    execution_requests_count,
    slot_timestamp
FROM 
    {{ source('consensus', 'blocks') }} FINAL
