


WITH

source AS (
    SELECT
    block_number,
    block_hash,
    transaction_index,
    log_index,
    transaction_hash,
    CONCAT('0x',address) AS address,
    CONCAT('0x',topic0) AS topic0,
    topic1,
    topic2,
    topic3,
    data,
    n_data_bytes,
    block_timestamp
    FROM `execution`.`logs`
)

SELECT
   *
FROM source