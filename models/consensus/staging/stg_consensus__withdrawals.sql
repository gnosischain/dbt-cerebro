{{
    config(
        materialized='view',
        tags=["production", "consensus", "withdrawals"]
    )
}}

SELECT
    slot,
    block_number,
    block_hash,
    withdrawal_index,
    validator_index,
    address,
    amount,
    slot_timestamp
FROM 
    {{ source('consensus', 'withdrawals') }} FINAL
