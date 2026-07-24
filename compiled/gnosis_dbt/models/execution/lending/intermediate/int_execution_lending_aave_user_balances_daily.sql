

-- depends_on: `dbt`.`int_execution_lending_aave_diffs_daily`
-- NOTE: scaled_balance and balance_raw are UInt256/Int256 for exact aToken math
-- (mirrors Aave's on-chain WadRayMath). Run with --full-refresh when migrating from
-- the previous Float64 schema so the column types are recreated.






WITH

reserve_map AS (
    SELECT
        protocol,
        lower(reserve_address) AS reserve_address,
        reserve_symbol,
        decimals
    FROM `dbt`.`lending_market_mapping`
    WHERE 1=1
      
  

),

deltas AS (
    SELECT
        d.date            AS date,
        d.protocol        AS protocol,
        d.user_address    AS user_address,
        d.reserve_address AS reserve_address,
        d.diff_scaled     AS diff_scaled
    FROM `dbt`.`int_execution_lending_aave_diffs_daily` d
    INNER JOIN reserve_map rm
      ON  rm.protocol        = d.protocol
     AND  rm.reserve_address = d.reserve_address
    WHERE d.date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(d.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_lending_aave_user_balances_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(d.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_lending_aave_user_balances_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

pool_events_raw AS (
    SELECT 'Aave V3'   AS protocol, * FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    UNION ALL
    SELECT 'SparkLend' AS protocol, * FROM `dbt`.`contracts_spark_Pool_events`
),

daily_index AS (
    SELECT
        protocol,
        toDate(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS reserve_address,
        argMax(
            toUInt256OrZero(decoded_params['liquidityIndex']),
            (block_timestamp, log_index)
        ) AS liquidity_index_eod
    FROM pool_events_raw
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex'] IS NOT NULL
      AND (protocol, lower(decoded_params['reserve'])) IN (
            SELECT protocol, reserve_address FROM reserve_map
      )
      AND block_timestamp < today()
      
    GROUP BY protocol, date, reserve_address
),

overall_max_date AS (
    SELECT
        least(
            
                yesterday(),
            
            yesterday()
        ) AS max_date
),


-- Append path (start_month / incremental_end_date): strict max(date) watermark
-- so the calendar only emits dates that are not already in the table.
-- Daily delete+insert path: lag one day (date < yesterday) so yesterday can be
-- recomputed when late diffs arrive.
current_partition AS (
    SELECT
        max(date) AS max_date
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
      
      AND date < yesterday()
      
),

prev_balances AS (
    -- Dedup the seed day with partition-pruned GROUP BY any() instead of FINAL.
    -- Unmerged ReplacingMergeTree parts on the seed day were what turned one
    -- carry-forward into two seeds and permanently doubled every holder's balance.
    SELECT
        protocol,
        user_address,
        reserve_address,
        any(scaled_balance) AS scaled_balance
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date = (SELECT max_date FROM current_partition)
      AND (protocol, reserve_address) IN (SELECT protocol, reserve_address FROM reserve_map)
    GROUP BY protocol, user_address, reserve_address
),

seed_date AS (
    SELECT max_date FROM current_partition
),




keys AS (
    SELECT DISTINCT
        protocol,
        user_address,
        reserve_address
    FROM (
        SELECT protocol, user_address, reserve_address FROM prev_balances
        UNION ALL
        SELECT protocol, user_address, reserve_address FROM deltas
    )
),

calendar AS (
    SELECT
        k.protocol        AS protocol,
        k.user_address    AS user_address,
        k.reserve_address AS reserve_address,
        addDays(sd.max_date, offset + 1) AS date
    FROM keys k
    CROSS JOIN seed_date sd
    CROSS JOIN overall_max_date omd
    ARRAY JOIN range(toUInt64(greatest(
        dateDiff('day', sd.max_date, omd.max_date),
        0
    ))) AS offset
),



daily_balances AS (
    SELECT
        c.date            AS date,
        c.protocol        AS protocol,
        c.user_address    AS user_address,
        c.reserve_address AS reserve_address,
        sum(coalesce(d.diff_scaled, toInt256(0))) OVER (
            PARTITION BY c.protocol, c.user_address, c.reserve_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.scaled_balance, toInt256(0))
        
        AS scaled_balance
    FROM calendar c
    LEFT JOIN deltas d
      ON  d.date            = c.date
     AND  d.protocol        = c.protocol
     AND  d.user_address    = c.user_address
     AND  d.reserve_address = c.reserve_address
    
    LEFT JOIN prev_balances p
      ON  p.protocol        = c.protocol
     AND  p.user_address    = c.user_address
     AND  p.reserve_address = c.reserve_address
    
),

balances_with_index AS (
    SELECT
        b.date                AS date,
        b.protocol            AS protocol,
        b.user_address        AS user_address,
        b.reserve_address     AS reserve_address,
        b.scaled_balance      AS scaled_balance,
        i.liquidity_index_eod AS liquidity_index_eod
    FROM daily_balances b
    ASOF LEFT JOIN daily_index i
        ON  i.protocol        = b.protocol
        AND i.reserve_address = b.reserve_address
        AND b.date >= i.date
    -- Sparse-table rule: drop zero balances, but on incremental runs still emit
    -- a zero row for keys that had activity in the window so delete+insert can
    -- overwrite a stale positive balance after a full withdraw.
    WHERE b.scaled_balance != toInt256(0)
    
       OR (b.protocol, b.user_address, b.reserve_address) IN (
            SELECT DISTINCT protocol, user_address, reserve_address FROM deltas
          )
    
),

balances_with_underlying AS (
    SELECT
        bi.date            AS date,
        bi.protocol        AS protocol,
        bi.user_address    AS user_address,
        bi.reserve_address AS reserve_address,
        rm.reserve_symbol  AS symbol,
        rm.decimals        AS decimals,
        bi.scaled_balance  AS scaled_balance,
        CASE
            WHEN bi.scaled_balance <= toInt256(0) THEN toUInt256OrZero('0')
            ELSE intDiv(
                toUInt256(bi.scaled_balance) * bi.liquidity_index_eod,
                toUInt256OrZero('1000000000000000000000000000')
            )
        END AS balance_raw
    FROM balances_with_index bi
    INNER JOIN reserve_map rm
        ON  rm.protocol        = bi.protocol
        AND rm.reserve_address = bi.reserve_address
)

SELECT
    b.date            AS date,
    b.protocol        AS protocol,
    b.reserve_address AS reserve_address,
    b.symbol          AS symbol,
    b.user_address    AS user_address,
    b.scaled_balance  AS scaled_balance,
    b.balance_raw     AS balance_raw,
    toFloat64(b.balance_raw) / power(10, b.decimals) AS balance,
    (toFloat64(b.balance_raw) / power(10, b.decimals)) * coalesce(p.price, 0) AS balance_usd
FROM balances_with_underlying b
LEFT JOIN `dbt`.`int_execution_token_prices_daily` p
    ON p.date = b.date
   AND p.symbol = b.symbol
WHERE b.date < today()
  