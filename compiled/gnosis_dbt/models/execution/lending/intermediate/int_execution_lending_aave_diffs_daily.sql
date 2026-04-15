

-- NOTE: diff_scaled is Int256 for exact scaled-balance arithmetic (no Float64 precision loss
-- on amounts above 2^53 wei). When deploying this model for the first time after the
-- Float64 -> Int256 migration, a --full-refresh is required so the column type is recreated.




WITH

reserve_map AS (
    SELECT
        lower(atoken_address)  AS atoken_address,
        lower(reserve_address) AS reserve_address,
        reserve_symbol,
        decimals
    FROM `dbt`.`atoken_reserve_mapping`
),

-- ReserveDataUpdated events carry the liquidityIndex snapshot that should be applied
-- to each pool action in the same tx. Order them within (tx, reserve) by log_index so
-- we can pair the N-th RDU with the N-th pool action (handles multi-action txs correctly).
reserve_index_by_tx AS (
    SELECT
        e.transaction_hash,
        lower(e.decoded_params['reserve']) AS reserve_address,
        e.log_index,
        toUInt256OrZero(e.decoded_params['liquidityIndex']) AS liquidity_index,
        row_number() OVER (
            PARTITION BY e.transaction_hash, lower(e.decoded_params['reserve'])
            ORDER BY e.log_index
        ) AS event_order
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events` e
    INNER JOIN reserve_map rm
        ON rm.reserve_address = lower(e.decoded_params['reserve'])
    WHERE e.event_name = 'ReserveDataUpdated'
      AND e.block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(e.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(e.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      
),

pool_events AS (
    SELECT
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['onBehalfOf']) AS user_address,
        'Supply' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'Supply'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'Withdraw' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'Withdraw'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS reserve_address,
        lower(decoded_params['repayer']) AS user_address,
        'RepayWithATokens' AS action,
        toUInt256OrZero(decoded_params['amount']) AS amount
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'Repay'
      -- decode_logs now emits bool as '0'/'1' (matching uint* convention);
      -- was silently matching 0 rows before the macro fix because decode_logs
      -- used to fall through to NULL for static bool params.
      AND decoded_params['useATokens'] = '1'
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['collateralAsset']) AS reserve_address,
        lower(decoded_params['user']) AS user_address,
        'LiquidationWithdraw' AS action,
        toUInt256OrZero(decoded_params['liquidatedCollateralAmount']) AS amount
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'LiquidationCall'
      -- Only count the burn case; when receiveAToken=true the collateral movement
      -- is already captured by aToken BalanceTransfer(borrower -> liquidator) below,
      -- so including those rows here would double-debit the borrower.
      AND decoded_params['receiveAToken'] = '0'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      
),

-- Filter to in-scope reserves and assign per-(tx, reserve) event_order so we can pair
-- each pool action with the ReserveDataUpdated snapshot that fires at the same position.
pool_events_ordered AS (
    SELECT
        pe.date             AS date,
        pe.transaction_hash AS transaction_hash,
        pe.log_index        AS log_index,
        pe.reserve_address  AS reserve_address,
        pe.user_address     AS user_address,
        pe.action           AS action,
        pe.amount           AS amount,
        row_number() OVER (
            PARTITION BY pe.transaction_hash, pe.reserve_address
            ORDER BY pe.log_index
        ) AS event_order
    FROM pool_events pe
    INNER JOIN reserve_map rm ON rm.reserve_address = pe.reserve_address
),

-- Convert pool actions to SCALED deltas using exact UInt256 arithmetic that matches
-- Aave's on-chain WadRayMath:
--   Supply              -> rayDivFloor(amount, index) = floor(amount * RAY / index)
--   Withdraw/Repay/Liq  -> rayDivCeil (amount, index) = floor((amount*RAY + index-1) / index)
pool_deltas AS (
    SELECT
        pe.date            AS date,
        pe.user_address    AS user_address,
        pe.reserve_address AS reserve_address,
        CASE
            WHEN pe.action = 'Supply' THEN
                toInt256(
                    intDiv(
                        pe.amount * toUInt256OrZero('1000000000000000000000000000'),
                        ri.liquidity_index
                    )
                )
            ELSE
                -toInt256(
                    intDiv(
                        pe.amount * toUInt256OrZero('1000000000000000000000000000')
                            + ri.liquidity_index - toUInt256OrZero('1'),
                        ri.liquidity_index
                    )
                )
        END AS scaled_delta
    FROM pool_events_ordered pe
    INNER JOIN reserve_index_by_tx ri
        ON ri.transaction_hash = pe.transaction_hash
       AND ri.reserve_address  = pe.reserve_address
       AND ri.event_order      = pe.event_order
    WHERE ri.liquidity_index > toUInt256OrZero('0')
),

-- aToken BalanceTransfer values are emitted by Aave already in scaled units, so they
-- flow straight into the Int256 ledger without any index conversion.
transfer_deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['from']) AS user_address,
        rm.reserve_address AS reserve_address,
        -toInt256(toUInt256OrZero(decoded_params['value'])) AS scaled_delta
    FROM `dbt`.`contracts_aaveV3_AToken_events` t
    INNER JOIN reserve_map rm
        ON rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      

    UNION ALL

    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['to']) AS user_address,
        rm.reserve_address AS reserve_address,
        toInt256(toUInt256OrZero(decoded_params['value'])) AS scaled_delta
    FROM `dbt`.`contracts_aaveV3_AToken_events` t
    INNER JOIN reserve_map rm
        ON rm.atoken_address = lower(t.contract_address)
    WHERE t.event_name = 'BalanceTransfer'
      AND decoded_params['from'] != '0x0000000000000000000000000000000000000000'
      AND decoded_params['to']   != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_diffs_daily` AS x2
      WHERE 1=1 
    )
  

      
),

all_deltas AS (
    SELECT date, user_address, reserve_address, scaled_delta
    FROM pool_deltas
    UNION ALL
    SELECT date, user_address, reserve_address, scaled_delta
    FROM transfer_deltas
),

agg AS (
    SELECT
        date,
        user_address,
        reserve_address,
        sum(scaled_delta) AS diff_scaled
    FROM all_deltas
    GROUP BY date, user_address, reserve_address
)

SELECT
    date,
    user_address,
    reserve_address,
    diff_scaled
FROM agg
WHERE diff_scaled != toInt256(0)