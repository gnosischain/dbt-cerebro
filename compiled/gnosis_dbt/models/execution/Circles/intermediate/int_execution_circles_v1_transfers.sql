






WITH deduped_logs AS (
    SELECT
        block_number,
        transaction_index,
        log_index,
        CONCAT('0x', transaction_hash) AS transaction_hash,
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        data,
        block_timestamp
    FROM (
        

SELECT block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp
FROM (
    SELECT
        block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index, log_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`logs`
    
    WHERE 
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND lower(concat('0x', address)) IN (
        SELECT token_id
        FROM `dbt`.`int_execution_circles_v1_avatars`
        WHERE token_id IS NOT NULL
    )
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v1_transfers` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v1_transfers` AS x2
        WHERE 1=1 
      )
    
  

    

    
)
WHERE _dedup_rn = 1

    )
),

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(address) AS token_address,
    lower(concat('0x', substring(topic1, 25, 40))) AS from_address,
    lower(concat('0x', substring(topic2, 25, 40))) AS to_address,
    reinterpretAsUInt256(reverse(unhex(replaceAll(data, '0x', '')))) AS amount_raw
FROM deduped_logs