


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
        `execution`.`blocks`
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
    
arrayFilter(
    x -> x != '',
    /* split on every “non word-ish” character (dash, @, space, etc.) */
    splitByRegexp(
        '[^A-Za-z0-9\\.]+',            -- ⇽ anything that isn’t a–z, 0–9 or “.”
        arrayStringConcat(
            arrayMap(
                i -> if(
                    reinterpretAsUInt8(substring(unhex(coalesce(extra_data, '')), i, 1)) BETWEEN 32 AND 126,
                    reinterpretAsString(substring(unhex(coalesce(extra_data, '')), i, 1)),
                    ' '
                ),
                range(1, length(unhex(coalesce(extra_data, ''))) + 1)
            ),
            ''
        )
    )
)
 AS decoded_extra_data,
    size,
    mix_hash,
    nonce,
    base_fee_per_gas,
    withdrawals_root,
    block_timestamp
FROM source