

-- NOTE: cumulative_scaled_supply and cumulative_scaled_borrow are Int256 for exact
-- WadRayMath. Run with --full-refresh when migrating from the previous Float64 schema.

WITH

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM `dbt`.`contracts_spark_Pool_events`
),

-- ReserveDataUpdated snapshots indexed by (protocol, tx, reserve, log_index). Each user
-- action is paired (via ASOF JOIN below) with the RDU immediately preceding it in the
-- same (tx, reserve) — this is correct for both Aave V3 (1 RDU per action) and Spark
-- (many RDUs interleaved from FlashLoans etc.), where rank-based pairing would break.
reserve_index_by_tx AS (
    SELECT
        protocol,
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        log_index,
        toUInt256OrZero(decoded_params['liquidityIndex'])       AS liquidity_index,
        toUInt256OrZero(decoded_params['variableBorrowIndex'])  AS variable_borrow_index
    FROM pool_events_raw
    WHERE event_name = 'ReserveDataUpdated'
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

),

supply_events AS (
    SELECT
        protocol,
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrZero(decoded_params['amount']) AS amount_raw
    FROM pool_events_raw
    WHERE event_name IN ('Supply', 'Withdraw')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  


    UNION ALL

    SELECT
        protocol,
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['collateralAsset']) AS token_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrZero(decoded_params['liquidatedCollateralAmount']) AS amount_raw
    FROM pool_events_raw
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

),

borrow_events AS (
    SELECT
        protocol,
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrZero(decoded_params['amount']) AS amount_raw
    FROM pool_events_raw
    WHERE event_name IN ('Borrow', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  


    UNION ALL

    SELECT
        protocol,
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['debtAsset']) AS token_address,
        'LiquidationRepay' AS event_type,
        toUInt256OrZero(decoded_params['debtToCover']) AS amount_raw
    FROM pool_events_raw
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['debtAsset'] IS NOT NULL
      AND decoded_params['debtToCover'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

),

supply_scaled AS (
    SELECT
        s.protocol,
        s.date,
        s.token_address,
        CASE
            WHEN s.event_type = 'Supply' THEN
                toInt256(
                    intDiv(
                        s.amount_raw * toUInt256OrZero('1000000000000000000000000000'),
                        r.liquidity_index
                    )
                )
            WHEN s.event_type IN ('Withdraw', 'LiquidationWithdraw') THEN
                -toInt256(
                    intDiv(
                        s.amount_raw * toUInt256OrZero('1000000000000000000000000000')
                            + r.liquidity_index - toUInt256OrZero('1'),
                        r.liquidity_index
                    )
                )
            ELSE toInt256(0)
        END AS scaled_delta
    FROM supply_events s
    ASOF INNER JOIN reserve_index_by_tx r
        ON  r.protocol         = s.protocol
        AND r.transaction_hash = s.transaction_hash
        AND r.token_address    = s.token_address
        AND r.log_index        <  s.log_index
    WHERE r.liquidity_index > toUInt256OrZero('0')
),

borrow_scaled AS (
    SELECT
        b.protocol,
        b.date,
        b.token_address,
        CASE
            WHEN b.event_type = 'Borrow' THEN
                toInt256(
                    intDiv(
                        b.amount_raw * toUInt256OrZero('1000000000000000000000000000'),
                        r.variable_borrow_index
                    )
                )
            WHEN b.event_type IN ('Repay', 'LiquidationRepay') THEN
                -toInt256(
                    intDiv(
                        b.amount_raw * toUInt256OrZero('1000000000000000000000000000')
                            + r.variable_borrow_index - toUInt256OrZero('1'),
                        r.variable_borrow_index
                    )
                )
            ELSE toInt256(0)
        END AS scaled_delta
    FROM borrow_events b
    ASOF INNER JOIN reserve_index_by_tx r
        ON  r.protocol         = b.protocol
        AND r.transaction_hash = b.transaction_hash
        AND r.token_address    = b.token_address
        AND r.log_index        <  b.log_index
    WHERE r.variable_borrow_index > toUInt256OrZero('0')
),

-- Build per-day deltas via UNION ALL + single GROUP BY. This avoids the FULL OUTER JOIN
-- path which had protocol-dimension correctness issues for Spark on ClickHouse (rows
-- where only supply or only borrow existed on a day would dilute the cumulative).
deltas AS (
    SELECT
        protocol,
        date,
        token_address,
        sum(scaled_delta) AS delta_supply,
        toInt256(0)       AS delta_borrow
    FROM supply_scaled
    GROUP BY protocol, date, token_address

    UNION ALL

    SELECT
        protocol,
        date,
        token_address,
        toInt256(0)       AS delta_supply,
        sum(scaled_delta) AS delta_borrow
    FROM borrow_scaled
    GROUP BY protocol, date, token_address
),

deltas_daily AS (
    SELECT
        protocol,
        date,
        token_address,
        sum(delta_supply) AS delta_supply,
        sum(delta_borrow) AS delta_borrow
    FROM deltas
    GROUP BY protocol, date, token_address
),


prev_cumulative AS (
    SELECT
        protocol,
        token_address,
        argMax(cumulative_scaled_supply, date) AS prev_supply,
        argMax(cumulative_scaled_borrow, date) AS prev_borrow
    FROM `dbt`.`int_execution_lending_aave_utilization_daily`
    GROUP BY protocol, token_address
),


with_cumulative AS (
    SELECT
        d.protocol,
        d.date,
        d.token_address,
        sum(d.delta_supply) OVER (
            PARTITION BY d.protocol, d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.prev_supply, toInt256(0))
        
        AS cumulative_scaled_supply,
        sum(d.delta_borrow) OVER (
            PARTITION BY d.protocol, d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.prev_borrow, toInt256(0))
        
        AS cumulative_scaled_borrow
    FROM deltas_daily d
    
    LEFT JOIN prev_cumulative p
        ON  p.protocol      = d.protocol
        AND p.token_address = d.token_address
    
),

daily_index AS (
    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        argMax(
            toUInt256OrZero(decoded_params['liquidityIndex']),
            (block_timestamp, log_index)
        ) AS liquidity_index_eod,
        argMax(
            toUInt256OrZero(decoded_params['variableBorrowIndex']),
            (block_timestamp, log_index)
        ) AS variable_borrow_index_eod
    FROM pool_events_raw
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex']      IS NOT NULL
      AND decoded_params['variableBorrowIndex'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_lending_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY protocol, date, token_address
)

SELECT
    c.date,
    c.protocol,
    c.token_address,
    c.cumulative_scaled_supply,
    c.cumulative_scaled_borrow,
    CASE
        WHEN c.cumulative_scaled_supply > toInt256(0)
             AND i.liquidity_index_eod        > toUInt256OrZero('0')
             AND i.variable_borrow_index_eod  > toUInt256OrZero('0')
        THEN
            toFloat64(
                toUInt256(c.cumulative_scaled_borrow) * i.variable_borrow_index_eod
            )
            / toFloat64(
                toUInt256(c.cumulative_scaled_supply) * i.liquidity_index_eod
            ) * 100
        ELSE NULL
    END AS utilization_rate
FROM with_cumulative c
LEFT JOIN daily_index i
    ON  i.protocol      = c.protocol
   AND  i.token_address = c.token_address
   AND  i.date          = c.date
ORDER BY c.date, c.protocol, c.token_address