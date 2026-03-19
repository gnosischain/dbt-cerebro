






WITH deduped_transactions AS (
    SELECT
        block_timestamp,
        CONCAT('0x', from_address) AS from_address
    FROM (
        

SELECT block_timestamp, from_address
FROM (
    SELECT
        block_timestamp, from_address,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`transactions`
    
    WHERE 
    block_timestamp < today()
    AND from_address IS NOT NULL
    AND success = 1
    
      AND toStartOfMonth(block_timestamp) >= (
        SELECT toStartOfMonth(max(first_seen_date))
        FROM `dbt`.`int_execution_transactions_unique_addresses`
      )
    

    
)
WHERE _dedup_rn = 1

    )
),

new_addresses AS (
    SELECT
        cityHash64(lower(from_address)) AS address_hash,
        min(toDate(block_timestamp))    AS first_seen_date
    FROM deduped_transactions
    GROUP BY address_hash
)

SELECT
    address_hash,
    first_seen_date
FROM new_addresses