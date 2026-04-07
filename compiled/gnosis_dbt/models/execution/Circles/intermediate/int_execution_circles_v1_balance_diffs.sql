



SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    from_address AS account,
    to_address AS counterparty,
    token_id,
    token_address,
    -toInt256(amount_raw) AS delta_raw,
    transfer_type
FROM `dbt`.`int_execution_circles_v1_transfers`
WHERE 1 = 1
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v1_balance_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v1_balance_diffs` AS x2
      WHERE 1=1 
    )
  

  

UNION ALL

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    to_address AS account,
    from_address AS counterparty,
    token_id,
    token_address,
    toInt256(amount_raw) AS delta_raw,
    transfer_type
FROM `dbt`.`int_execution_circles_v1_transfers`
WHERE 1 = 1
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v1_balance_diffs` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v1_balance_diffs` AS x2
      WHERE 1=1 
    )
  

  