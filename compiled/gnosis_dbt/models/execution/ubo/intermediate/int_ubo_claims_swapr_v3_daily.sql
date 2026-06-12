







WITH

-- ─── NPM OWNERSHIP TRACKING ────────────────────────────────────────────
npm_transfers AS (
    SELECT
        toUInt256OrNull(decoded_params['tokenId'])   AS token_id,
        lower(decoded_params['from'])                AS transfer_from,
        lower(decoded_params['to'])                  AS transfer_to,
        block_timestamp,
        transaction_hash,
        log_index
    FROM `dbt`.`contracts_Swapr_v3_NonfungiblePositionManager_events`
    WHERE event_name = 'Transfer'
      AND decoded_params['tokenId'] IS NOT NULL
      AND block_timestamp < today()
),

npm_current_owners AS (
    SELECT
        token_id,
        argMax(transfer_to, (block_timestamp, log_index)) AS current_owner
    FROM npm_transfers
    GROUP BY token_id
),

-- ─── NPM LIQUIDITY DELTAS ──────────────────────────────────────────────
-- Swapr IncreaseLiquidity includes the pool address directly.
-- DecreaseLiquidity does not, so we build a tokenId → pool map from
-- IncreaseLiquidity events.
npm_increase_events AS (
    SELECT
        toUInt256OrNull(decoded_params['tokenId'])   AS token_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'IncreaseLiquidity'                          AS event_name,
        toUInt256OrNull(decoded_params['amount0'])   AS amount0,
        toUInt256OrNull(decoded_params['amount1'])   AS amount1,
        lower(decoded_params['pool'])                AS pool_address
    FROM `dbt`.`contracts_Swapr_v3_NonfungiblePositionManager_events`
    WHERE event_name = 'IncreaseLiquidity'
      AND decoded_params['tokenId'] IS NOT NULL
      AND block_timestamp < today()
),

npm_decrease_events AS (
    SELECT
        toUInt256OrNull(decoded_params['tokenId'])   AS token_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'DecreaseLiquidity'                          AS event_name,
        toUInt256OrNull(decoded_params['amount0'])   AS amount0,
        toUInt256OrNull(decoded_params['amount1'])   AS amount1
    FROM `dbt`.`contracts_Swapr_v3_NonfungiblePositionManager_events`
    WHERE event_name = 'DecreaseLiquidity'
      AND decoded_params['tokenId'] IS NOT NULL
      AND block_timestamp < today()
),

-- ─── MAP tokenId → pool_address FROM IncreaseLiquidity ─────────────────
token_id_pool_map AS (
    SELECT
        token_id,
        argMin(pool_address, (block_timestamp, log_index)) AS pool_address
    FROM (
        SELECT token_id, pool_address, block_timestamp, log_index
        FROM npm_increase_events
        WHERE pool_address != ''
    )
    GROUP BY token_id
),

-- ─── POOL REGISTRY FOR TOKEN ADDRESSES ──────────────────────────────────
pool_tokens AS (
    SELECT
        pool_address,
        token0_address,
        token1_address
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Swapr V3'
),

-- ─── Combine increase + decrease into one stream ────────────────────────
npm_liquidity_events AS (
    SELECT token_id, block_timestamp, event_name, amount0, amount1,
           pool_address
    FROM npm_increase_events

    UNION ALL

    SELECT d.token_id, d.block_timestamp, d.event_name, d.amount0, d.amount1,
           tpm.pool_address
    FROM npm_decrease_events d
    INNER JOIN token_id_pool_map tpm ON tpm.token_id = d.token_id
),

-- ─── TRACK B: NPM-mediated LP deltas ───────────────────────────────────
npm_daily_deltas_raw AS (
    SELECT
        toDate(nle.block_timestamp)                                     AS date,
        lower(own.current_owner)                                        AS ubo_address,
        nle.pool_address                                                AS pool_address,
        nle.event_name,
        nle.amount0,
        nle.amount1
    FROM npm_liquidity_events nle
    INNER JOIN npm_current_owners own
        ON own.token_id = nle.token_id
    WHERE own.current_owner != '0x0000000000000000000000000000000000000000'
      AND nle.pool_address != ''
),

npm_daily_deltas AS (
    SELECT
        d.date,
        d.ubo_address,
        d.pool_address                                                  AS container_address,
        pt.token0_address                                               AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(d.event_name = 'IncreaseLiquidity',
                toInt256(d.amount0),
               -toInt256(d.amount0)))                                   AS daily_delta_raw
    FROM npm_daily_deltas_raw d
    INNER JOIN pool_tokens pt ON pt.pool_address = d.pool_address
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON  lower(tw.address)  = lower(pt.token0_address)
        AND d.date             >= tw.date_start
        AND (tw.date_end IS NULL OR d.date < tw.date_end)
    WHERE 1=1
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(d.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(d.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY d.date, d.ubo_address, d.pool_address, pt.token0_address, tw.symbol
    HAVING daily_delta_raw != 0

    UNION ALL

    SELECT
        d.date,
        d.ubo_address,
        d.pool_address                                                  AS container_address,
        pt.token1_address                                               AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(d.event_name = 'IncreaseLiquidity',
                toInt256(d.amount1),
               -toInt256(d.amount1)))                                   AS daily_delta_raw
    FROM npm_daily_deltas_raw d
    INNER JOIN pool_tokens pt ON pt.pool_address = d.pool_address
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON  lower(tw.address)  = lower(pt.token1_address)
        AND d.date             >= tw.date_start
        AND (tw.date_end IS NULL OR d.date < tw.date_end)
    WHERE 1=1
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(d.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(d.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY d.date, d.ubo_address, d.pool_address, pt.token1_address, tw.symbol
    HAVING daily_delta_raw != 0
),

-- ─── TRACK A: Direct LP deltas (owner != NPM) ──────────────────────────
direct_daily_deltas AS (
    SELECT
        toDate(liq.block_timestamp)                                     AS date,
        lower(liq.provider)                                             AS ubo_address,
        lower(liq.pool_address)                                         AS container_address,
        lower(liq.token_address)                                        AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(liq.event_type = 'mint',
                toInt256(liq.amount_raw),
               -toInt256(liq.amount_raw)))                              AS daily_delta_raw
    FROM `dbt`.`stg_pools__dex_liquidity_swapr_v3` liq
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON  lower(tw.address)           = lower(liq.token_address)
        AND toDate(liq.block_timestamp) >= tw.date_start
        AND (tw.date_end IS NULL OR toDate(liq.block_timestamp) < tw.date_end)
    WHERE liq.event_type IN ('mint', 'burn')
      AND lower(liq.provider) != lower('0x91fd594c46d8b01e62dbdebed2401dde01817834')
      AND liq.block_timestamp < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(liq.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(liq.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_ubo_claims_swapr_v3_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY date, ubo_address, container_address, token_address, tw.symbol
    HAVING daily_delta_raw != 0
),

-- ─── MERGE BOTH TRACKS ─────────────────────────────────────────────────
daily_deltas_agg AS (
    SELECT
        date,
        ubo_address,
        container_address,
        symbol,
        sum(daily_delta_raw) AS daily_delta_raw
    FROM (
        SELECT date, ubo_address, container_address, token_address, symbol, daily_delta_raw
        FROM npm_daily_deltas

        UNION ALL

        SELECT date, ubo_address, container_address, token_address, symbol, daily_delta_raw
        FROM direct_daily_deltas
    )
    GROUP BY date, ubo_address, container_address, symbol
),

overall_max_date AS (
    SELECT
        
            yesterday()
         AS max_date
),


current_partition AS (
    SELECT max(date) AS max_date
    FROM `dbt`.`int_ubo_claims_swapr_v3_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.container_address,
        t1.balance_raw
    FROM (SELECT ubo_address, token_address, container_address, balance_raw, date FROM `dbt`.`int_ubo_claims_swapr_v3_daily`) t1
    CROSS JOIN current_partition t2
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = t2.max_date
),



keys AS (
    SELECT DISTINCT ubo_address, symbol, container_address
    FROM (
        SELECT ubo_address, symbol, container_address FROM prev_balances
        UNION ALL
        SELECT ubo_address, symbol, container_address FROM daily_deltas_agg
    )
),

calendar AS (
    SELECT
        k.ubo_address,
        k.symbol,
        k.container_address,
        
            addDays(cp.max_date, offset + 1) AS date
        
    FROM keys k
    
    CROSS JOIN current_partition cp
    
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        toUInt32(dateDiff('day',
            
                cp.max_date,
            
            o.max_date
        ))
    ) AS offset
),


balances AS (
    SELECT
        c.date             AS date,
        c.ubo_address      AS ubo_address,
        c.symbol           AS symbol,
        c.container_address AS container_address,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.ubo_address, c.symbol, c.container_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.balance_raw, toInt256(0))
        
        AS balance_raw
    FROM calendar c
    LEFT JOIN daily_deltas_agg d
        ON  d.ubo_address       = c.ubo_address
        AND d.symbol            = c.symbol
        AND d.container_address = c.container_address
        AND d.date              = c.date
    
    LEFT JOIN prev_balances p
        ON  p.ubo_address       = c.ubo_address
        AND p.symbol            = c.symbol
        AND p.container_address = c.container_address
    
)

SELECT
    b.date                                                                  AS date,
    'Swapr V3'                                                              AS protocol,
    lower(b.container_address)                                              AS container_address,
    lower(tw_canon.address)                                                 AS token_address,
    b.symbol                                                                AS symbol,
    tw_canon.token_class                                                    AS token_class,
    lower(b.ubo_address)                                                    AS ubo_address,
    toInt256(b.balance_raw)                                                 AS balance_raw,
    b.balance_raw / pow(10, tw_canon.decimals)                             AS balance,
    (b.balance_raw / pow(10, tw_canon.decimals)) * coalesce(pr.price, 0)  AS balance_usd
FROM balances b
INNER JOIN `dbt`.`tokens_whitelist` tw_canon
    ON  tw_canon.symbol = b.symbol
    AND b.date          >= tw_canon.date_start
    AND (tw_canon.date_end IS NULL OR b.date < tw_canon.date_end)
ASOF LEFT JOIN (
    SELECT symbol, date, price
    FROM `dbt`.`int_execution_token_prices_daily`
    ORDER BY symbol, date
) pr
    ON  pr.symbol = b.symbol
    AND b.date    >= pr.date
WHERE b.balance_raw > 0