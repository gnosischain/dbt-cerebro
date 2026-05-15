

-- Per-transfer categorisation. Wraps int_execution_circles_v2_transfers and
-- tags each row with one of five `transfer_category` values:
--
--   mint        - Hub ERC-1155 TransferSingle, from = 0x00..00
--   burn        - Hub ERC-1155 TransferSingle, to   = 0x00..00
--   wrap        - Wrapper ERC-20 Transfer,     from = 0x00..00
--   unwrap      - Wrapper ERC-20 Transfer,     to   = 0x00..00
--   p2p         - any other transfer (peer-to-peer)
--
-- The plan calls for splitting p2p into `p2p_direct` and `p2p_matrix`
-- (matrix-routed via OperatorMatrixFlow → StreamCompleted), but the
-- StreamCompleted event isn't decoded into contracts_circles_v2_Hub_events
-- yet. Once it lands, add a SEMI JOIN against int_execution_circles_v2_stream_completed
-- on (transaction_hash) and split `p2p` into the two subcategories here.




SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    transfer_type,
    from_address,
    to_address,
    token_address,
    amount_raw,
    amount_demurraged_raw,
    multiIf(
        transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND from_address = '0x0000000000000000000000000000000000000000', 'wrap',
        transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND to_address   = '0x0000000000000000000000000000000000000000', 'unwrap',
        from_address = '0x0000000000000000000000000000000000000000', 'mint',
        to_address   = '0x0000000000000000000000000000000000000000', 'burn',
        'p2p'
    ) AS transfer_category
FROM `dbt`.`int_execution_circles_v2_transfers`
WHERE block_timestamp < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_transfers_categorised` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_transfers_categorised` AS x2
        WHERE 1=1 
      )
    
  

  