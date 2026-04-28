



WITH single_rows AS (
    SELECT
        block_number,
        block_timestamp,
        lower(concat('0x', transaction_hash)) AS transaction_hash,
        transaction_index,
        log_index,
        0 AS batch_index,
        lower(decoded_params['operator']) AS operator,
        lower(decoded_params['from']) AS from_address,
        lower(decoded_params['to']) AS to_address,
        toString(toUInt256OrZero(decoded_params['id'])) AS token_id,
        toUInt256OrZero(decoded_params['value']) AS amount_raw,
        concat(
  '0x',
  leftPad(lower(hex(toUInt256(token_id))), 40, '0')
) AS token_address,
        'CrcV2_TransferSingle' AS transfer_type
    FROM `dbt`.`contracts_circles_v2_Hub_events`
    WHERE event_name = 'TransferSingle'
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_hub_transfers` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_hub_transfers` AS x2
        WHERE 1=1 
      )
    
  

      
),
batch_rows AS (
    SELECT
        block_number,
        block_timestamp,
        lower(concat('0x', transaction_hash)) AS transaction_hash,
        transaction_index,
        log_index,
        arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')) AS batch_indexes,
        lower(decoded_params['operator']) AS operator,
        lower(decoded_params['from']) AS from_address,
        lower(decoded_params['to']) AS to_address,
        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)') AS token_ids,
        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)') AS values
    FROM `dbt`.`contracts_circles_v2_Hub_events`
    WHERE event_name = 'TransferBatch'
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_hub_transfers` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_hub_transfers` AS x2
        WHERE 1=1 
      )
    
  

      
),
exploded_batch_rows AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        operator,
        from_address,
        to_address,
        batch_tuple.2.1 AS token_id,
        toUInt256(batch_tuple.2.2) AS amount_raw,
        concat(
  '0x',
  leftPad(lower(hex(toUInt256(batch_tuple.2.1))), 40, '0')
) AS token_address,
        'CrcV2_TransferBatch' AS transfer_type
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            operator,
            from_address,
            to_address,
            arrayJoin(arrayZip(batch_indexes, arrayZip(token_ids, values))) AS batch_tuple
        FROM batch_rows
    )
)

SELECT * FROM single_rows
UNION ALL
SELECT * FROM exploded_batch_rows