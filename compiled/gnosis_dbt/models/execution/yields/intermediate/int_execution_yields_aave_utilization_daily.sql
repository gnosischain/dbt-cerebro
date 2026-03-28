

WITH

reserve_index_by_tx AS (
    SELECT
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        any(toFloat64(toUInt256OrNull(decoded_params['liquidityIndex']))) AS liquidity_index,
        any(toFloat64(toUInt256OrNull(decoded_params['variableBorrowIndex']))) AS variable_borrow_index
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'ReserveDataUpdated'
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY transaction_hash, lower(decoded_params['reserve'])
),

supply_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name IN ('Supply', 'Withdraw')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  


    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['collateralAsset']) AS token_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrNull(decoded_params['liquidatedCollateralAmount']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

),

borrow_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name IN ('Borrow', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  


    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['debtAsset']) AS token_address,
        'LiquidationRepay' AS event_type,
        toUInt256OrNull(decoded_params['debtToCover']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['debtAsset'] IS NOT NULL
      AND decoded_params['debtToCover'] IS NOT NULL
      AND block_timestamp < today()
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_yields_aave_utilization_daily` AS x2
      WHERE 1=1 
    )
  

),

supply_scaled AS (
    SELECT
        s.date,
        s.token_address,
        CASE
            WHEN s.event_type = 'Supply' THEN toFloat64(s.amount_raw) * 1e27 / r.liquidity_index
            WHEN s.event_type IN ('Withdraw', 'LiquidationWithdraw') THEN -toFloat64(s.amount_raw) * 1e27 / r.liquidity_index
            ELSE 0
        END AS scaled_delta
    FROM supply_events s
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = s.transaction_hash
       AND r.token_address = s.token_address
    WHERE r.liquidity_index IS NOT NULL
      AND r.liquidity_index > 0
),

borrow_scaled AS (
    SELECT
        b.date,
        b.token_address,
        CASE
            WHEN b.event_type = 'Borrow' THEN toFloat64(b.amount_raw) * 1e27 / r.variable_borrow_index
            WHEN b.event_type IN ('Repay', 'LiquidationRepay') THEN -toFloat64(b.amount_raw) * 1e27 / r.variable_borrow_index
            ELSE 0
        END AS scaled_delta
    FROM borrow_events b
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = b.transaction_hash
       AND r.token_address = b.token_address
    WHERE r.variable_borrow_index IS NOT NULL
      AND r.variable_borrow_index > 0
),

supply_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS delta_supply
    FROM supply_scaled
    GROUP BY date, token_address
),

borrow_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS delta_borrow
    FROM borrow_scaled
    GROUP BY date, token_address
),

deltas AS (
    SELECT
        coalesce(s.date, b.date) AS date,
        coalesce(s.token_address, b.token_address) AS token_address,
        coalesce(s.delta_supply, 0) AS delta_supply,
        coalesce(b.delta_borrow, 0) AS delta_borrow
    FROM supply_daily s
    FULL OUTER JOIN borrow_daily b
        ON b.date = s.date
       AND b.token_address = s.token_address
),


prev_cumulative AS (
    SELECT
        token_address,
        argMax(cumulative_scaled_supply, date) AS prev_supply,
        argMax(cumulative_scaled_borrow, date) AS prev_borrow
    FROM `dbt`.`int_execution_yields_aave_utilization_daily`
    GROUP BY token_address
),


with_cumulative AS (
    SELECT
        d.date,
        d.token_address,
        sum(d.delta_supply) OVER (
            PARTITION BY d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.prev_supply, 0)
        
        AS cumulative_scaled_supply,
        sum(d.delta_borrow) OVER (
            PARTITION BY d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.prev_borrow, 0)
        
        AS cumulative_scaled_borrow
    FROM deltas d
    
    LEFT JOIN prev_cumulative p
        ON p.token_address = d.token_address
    
),

daily_index AS (
    SELECT
        date,
        token_address,
        liquidity_index,
        variable_borrow_index
    FROM `dbt`.`int_execution_yields_aave_daily`
    WHERE liquidity_index IS NOT NULL
      AND variable_borrow_index IS NOT NULL
)

SELECT
    c.date,
    c.token_address,
    c.cumulative_scaled_supply,
    c.cumulative_scaled_borrow,
    CASE
        WHEN c.cumulative_scaled_supply > 0
             AND i.liquidity_index IS NOT NULL
             AND i.variable_borrow_index IS NOT NULL
        THEN (c.cumulative_scaled_borrow * i.variable_borrow_index)
             / (c.cumulative_scaled_supply * i.liquidity_index) * 100
        ELSE NULL
    END AS utilization_rate
FROM with_cumulative c
LEFT JOIN daily_index i
    ON i.token_address = c.token_address
   AND i.date = c.date
ORDER BY c.date, c.token_address