WITH


source AS (
    SELECT 
        block_number,
        transaction_index,
        CONCAT('0x', transaction_hash) AS transaction_hash,
        CONCAT('0x', address) AS address,
        slot,
        from_value,
        to_value,
        block_timestamp
    FROM 
        {{ source('execution','storage_diffs') }}
)

SELECT
    *
FROM source

        
