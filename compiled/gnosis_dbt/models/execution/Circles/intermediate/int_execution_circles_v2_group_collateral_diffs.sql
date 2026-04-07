



WITH single_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        0 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        toString(toUInt256OrZero(decoded_params['id'])) AS token_id,
        toInt256(toUInt256OrZero(decoded_params['value'])) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM `dbt`.`contracts_circles_v2_StandardTreasury_events`
    WHERE event_name = 'CollateralLockedSingle'
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x2
      WHERE 1=1 
    )
  

      
),
batch_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM `dbt`.`contracts_circles_v2_StandardTreasury_events`
        WHERE event_name = 'CollateralLockedBatch'
          
            
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x2
      WHERE 1=1 
    )
  

          
    )
),
batch_burns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM `dbt`.`contracts_circles_v2_StandardTreasury_events`
        WHERE event_name = 'GroupRedeemCollateralBurn'
          
            
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x2
      WHERE 1=1 
    )
  

          
    )
),
batch_returns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        lower(decoded_params['to']) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM `dbt`.`contracts_circles_v2_StandardTreasury_events`
        WHERE event_name = 'GroupRedeemCollateralReturn'
          
            
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs` AS x2
      WHERE 1=1 
    )
  

          
    )
)

SELECT * FROM single_locks
UNION ALL
SELECT * FROM batch_locks
UNION ALL
SELECT * FROM batch_burns
UNION ALL
SELECT * FROM batch_returns