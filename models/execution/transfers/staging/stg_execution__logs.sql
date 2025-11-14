{{
    config(
        materialized='view',
        tags=['production', 'execution', 'logs']
    )
}}

WITH source AS (
    SELECT
        block_number,
        block_hash,
        transaction_index,
        log_index,
        transaction_hash,
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        data,
        n_data_bytes,
        chain_id,
        block_timestamp,
        insert_version
    FROM {{ source('execution','logs') }} FINAL
)

SELECT
    block_number,
    block_hash,
    transaction_index,
    log_index,
    transaction_hash,
    address,
    topic0,
    topic1,
    topic2,
    topic3,
    data,
    n_data_bytes,
    chain_id,
    block_timestamp,
    insert_version
FROM source