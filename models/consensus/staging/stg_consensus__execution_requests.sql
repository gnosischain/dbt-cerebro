{{
    config(
        materialized='view',
        tags=["production", "consensus", "execution_requests"]
    )
}}

SELECT
    slot,
    payload,
    deposits_count,
    withdrawals_count,
    consolidations_count,
    slot_timestamp
FROM 
    {{ source('consensus', 'execution_requests') }} FINAL
