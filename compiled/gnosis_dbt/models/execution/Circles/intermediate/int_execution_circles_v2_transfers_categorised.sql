

-- Per-transfer categorisation. Wraps int_execution_circles_v2_transfers and
-- tags each row with one of five `transfer_category` values:
--
--   mint        - Hub ERC-1155 TransferSingle, from = 0x00..00
--   burn        - Hub ERC-1155 TransferSingle, to   = 0x00..00
--   wrap        - Wrapper ERC-20 Transfer,     from = 0x00..00
--   unwrap      - Wrapper ERC-20 Transfer,     to   = 0x00..00
--   p2p         - any other transfer (peer-to-peer)
--
-- For `mint` rows, the `mint_kind` column further distinguishes personal
-- mints, group mints, and V1→V2 migrations — sourced from
-- int_execution_circles_v2_mint_events. NULL for non-mint rows.
--
-- The plan calls for splitting p2p into `p2p_direct` and `p2p_matrix`
-- (matrix-routed via OperatorMatrixFlow → StreamCompleted), but the
-- StreamCompleted event isn't decoded into contracts_circles_v2_Hub_events
-- yet. Once it lands, add a SEMI JOIN against int_execution_circles_v2_stream_completed
-- on (transaction_hash) and split `p2p` into the two subcategories here.




WITH base AS (
    SELECT *
    FROM `dbt`.`int_execution_circles_v2_transfers`
    WHERE block_timestamp < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_transfers_categorised` AS x1
        WHERE 1=1 
      )
    
  

      
),
mints AS (
    -- Pre-tagged mint flavours, deduped to one row per (tx, recipient, token)
    -- so the LEFT JOIN below cannot fan out a base row. Keyed on
    -- (tx, to_address, token_address) because int_execution_circles_v2_mint_events
    -- is now sourced from the PersonalMint / GroupMint events, whose log_index
    -- differs from the TransferSingle mint-leg log_index in `base`.
    SELECT
        transaction_hash,
        to_address,
        token_address,
        any(mint_kind) AS mint_kind
    FROM `dbt`.`int_execution_circles_v2_mint_events`
    WHERE block_timestamp < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_transfers_categorised` AS x1
        WHERE 1=1 
      )
    
  

      
    GROUP BY transaction_hash, to_address, token_address
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.transaction_hash,
    b.transaction_index,
    b.log_index,
    b.batch_index,
    b.transfer_type,
    b.from_address,
    b.to_address,
    b.token_address,
    b.amount_raw,
    b.amount_demurraged_raw,
    multiIf(
        b.transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND b.from_address = '0x0000000000000000000000000000000000000000', 'wrap',
        b.transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND b.to_address   = '0x0000000000000000000000000000000000000000', 'unwrap',
        b.from_address = '0x0000000000000000000000000000000000000000', 'mint',
        b.to_address   = '0x0000000000000000000000000000000000000000', 'burn',
        'p2p'
    ) AS transfer_category,
    if(
        b.from_address = '0x0000000000000000000000000000000000000000'
        AND b.transfer_type != 'CrcV2_ERC20WrapperTransfer',
        m.mint_kind,
        NULL
    ) AS mint_kind
FROM base b
LEFT JOIN mints m
    ON  m.transaction_hash = b.transaction_hash
    AND m.to_address       = b.to_address
    AND m.token_address    = b.token_address