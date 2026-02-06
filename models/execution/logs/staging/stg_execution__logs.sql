{{
    config(
        materialized='view',
        tags=['production', 'execution', 'logs']
    )
}}


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
    FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY block_number, transaction_index, log_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM {{ source('execution','logs') }}
    )
    WHERE _dedup_rn = 1
)

SELECT
   *
FROM source
