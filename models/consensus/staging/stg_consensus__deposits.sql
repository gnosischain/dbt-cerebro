{{
    config(
        materialized='view',
        tags=["production", "consensus", "deposits"]
    )
}}

SELECT
   slot,
    deposit_index,
    pubkey,
    withdrawal_credentials,
    amount,
    signature,
    proof,
    slot_timestamp
FROM 
    {{ source('consensus', 'deposits') }} FINAL
