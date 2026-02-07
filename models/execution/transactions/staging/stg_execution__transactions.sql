{{ 
    config(
        materialized='view',
        tags=['production', 'execution', 'transactions']
    )
}}

WITH

source AS (
    SELECT
        *
    FROM (
        SELECT 
            block_number,
            transaction_index,
            transaction_hash,
            nonce,
            from_address,
            to_address,
            value_string,
            input,
            gas_limit,
            gas_used,
            gas_price,
            transaction_type,
            max_priority_fee_per_gas,
            max_fee_per_gas,
            success,
            n_input_bytes,
            n_input_zero_bytes,
            n_input_nonzero_bytes,
            n_rlp_bytes,
            r,
            s,
            v,
            block_hash,
            block_timestamp,
            row_number() OVER (
                PARTITION BY block_number, transaction_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM {{ source('execution','transactions') }}
    )
    WHERE _dedup_rn = 1
)

SELECT
    block_number,
    transaction_index,
    transaction_hash,
    nonce,
    CONCAT('0x',from_address) AS from_address,
    IF(to_address IS NULL, NULL, CONCAT('0x',to_address)) AS to_address,
    CAST(value_string AS UInt256) AS value,
    input,
    gas_limit,
    gas_used,
    gas_price,
    transaction_type,
    max_priority_fee_per_gas,
    max_fee_per_gas,
    success,
    n_input_bytes,
    n_input_zero_bytes,
    n_input_nonzero_bytes,
    n_rlp_bytes,
    r,
    s,
    v,
    block_hash,
    block_timestamp
FROM source
