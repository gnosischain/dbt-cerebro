




SELECT DISTINCT
    t.block_timestamp,
    t.transaction_hash,
    lower(t.from_address) AS tx_from,
    lower(t.to_address)   AS tx_to
FROM `execution`.`transactions` t
WHERE t.transaction_hash IN (
    SELECT DISTINCT transaction_hash
    FROM `dbt`.`int_execution_pools_dex_trades_raw`
    
      
  
    
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_pools_dex_trades_tx_context` AS x1
        WHERE 1=1 
      )
      
    
  

    
)

  
  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_pools_dex_trades_tx_context` AS x1
        WHERE 1=1 
      )
      
    
  

