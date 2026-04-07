



-- Hub ERC-1155 transfers (always in demurrage units)
SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    operator,
    from_address,
    to_address,
    token_address,
    amount_raw,
    amount_raw AS amount_demurraged_raw,
    'demurrage' AS unit_type,
    transfer_type
FROM `dbt`.`int_execution_circles_v2_hub_transfers`
WHERE 1 = 1
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_transfers` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_transfers` AS x2
      WHERE 1=1 
    )
  

  

UNION ALL

-- Wrapper ERC-20 transfers (static amounts converted to demurrage)
SELECT
    wt.block_number,
    wt.block_timestamp,
    wt.transaction_hash,
    wt.transaction_index,
    wt.log_index,
    0 AS batch_index,
    '' AS operator,
    wt.from_address,
    wt.to_address,
    wt.token_address,
    wt.amount_raw,
    if(w.circles_type = 1,
       toUInt256(
           multiplyDecimal(
               toDecimal256(wt.amount_raw, 0),
               
toDecimal256(
  pow(
    toDecimal256('0.9998013320085989574306481700129226782902039065082930593676448873', 64),
    intDiv(toUInt64(toUnixTimestamp(wt.block_timestamp)) - 1602720000, 86400)
    - intDiv(1602720000 - 1602720000, 86400)
  ),
  18
),
               0
           )
       ),
       wt.amount_raw
    ) AS amount_demurraged_raw,
    if(w.circles_type = 1, 'static', 'demurrage') AS unit_type,
    'CrcV2_ERC20WrapperTransfer' AS transfer_type
FROM `dbt`.`int_execution_circles_v2_wrapper_transfers` wt
INNER JOIN `dbt`.`int_execution_circles_v2_wrappers` w
    ON wt.token_address = w.wrapper_address
WHERE 1 = 1
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(wt.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v2_transfers` AS x1
      WHERE 1=1 
    )
    AND toDate(wt.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v2_transfers` AS x2
      WHERE 1=1 
    )
  

  