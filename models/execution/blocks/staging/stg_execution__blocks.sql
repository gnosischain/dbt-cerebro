{{ 
    config(
        materialized='view',
        tags=['production','execution','blocks']
    )
}}


WITH

source AS (
    SELECT 
        block_number,
        block_hash,
        parent_hash,
        uncles_hash,
        author,
        state_root,
        transactions_root,
        receipts_root,
        gas_used,
        gas_limit,
        extra_data,
        size,
        mix_hash,
        nonce,
        base_fee_per_gas,
        withdrawals_root,
        block_timestamp
    FROM 
        {{ source('execution','blocks') }}
    WHERE 
        block_timestamp > '1970-01-01' -- remove genesis
)

SELECT
    block_number,
    block_hash,
    parent_hash,
    uncles_hash,
    CONCAT('0x',author) AS author,
    state_root,
    transactions_root,
    receipts_root,
    gas_used,
    gas_limit,
    extra_data,
    {{ decode_hex_tokens('extra_data') }} AS decoded_extra_data,
    size,
    mix_hash,
    nonce,
    base_fee_per_gas,
    withdrawals_root,
    block_timestamp
FROM source


        
