



WITH

pools AS (
    SELECT
        pool_address,
        replaceAll(pool_address, '0x', '') AS pool_address_no0x,
        token0_address,
        token1_address
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Swapr V3'
      AND pool_address IN (
          SELECT lower(address)
          FROM `dbt`.`contracts_whitelist`
          WHERE contract_type = 'SwaprPool'
      )
),

/* -- Swapr V3 dynamic fee schedule -- */
fee_events AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        (toUInt64(toUnixTimestamp(block_timestamp)) * toUInt64(4294967296) + toUInt64(log_index)) AS event_order,
        toUInt32OrNull(decoded_params['fee']) AS fee_ppm
    FROM `dbt`.`contracts_Swapr_v3_AlgebraPool_events`
    WHERE event_name = 'Fee'
      AND decoded_params['fee'] IS NOT NULL
),

first_fee AS (
    SELECT
        pool_address_no0x,
        argMin(fee_ppm, event_order) AS first_fee_ppm
    FROM fee_events
    WHERE fee_ppm IS NOT NULL
    GROUP BY pool_address_no0x
),

daily_deltas AS (
    SELECT
        date,
        pool_address,
        token_address,
        sum(multiIf(
            delta_category = 'liquidity' AND delta_amount_raw < toInt256(0),
                toInt256(0),
            delta_amount_raw
        )) AS daily_delta_raw,
        sum(multiIf(
            delta_category = 'swap_in',
                delta_amount_raw - intDiv(delta_amount_raw * toInt256(effective_fee_ppm), toInt256(1000000)),
            delta_category IN ('fee_collection', 'flash_fee'),
                toInt256(0),
            delta_amount_raw
        )) AS daily_reserve_delta_raw,
        sum(multiIf(
            delta_category = 'swap_in',
                intDiv(delta_amount_raw * toInt256(effective_fee_ppm), toInt256(1000000)),
            delta_category IN ('fee_collection', 'flash_fee'),
                delta_amount_raw,
            toInt256(0)
        )) AS daily_fee_delta_raw
    FROM (
        SELECT
            sw.date AS date,
            sw.pool_address AS pool_address,
            sw.token_address AS token_address,
            sw.delta_amount_raw AS delta_amount_raw,
            sw.delta_category AS delta_category,
            toUInt32(if(f.fee_ppm > 0, f.fee_ppm, coalesce(ff.first_fee_ppm, 0))) AS effective_fee_ppm
        FROM (
            SELECT
                concat('0x', e.pool_address) AS pool_address,
                e.pool_address AS pool_address_no0x,
                multiIf(
                    e.token_position = 'token0', p.token0_address,
                    e.token_position = 'token1', p.token1_address,
                    NULL
                ) AS token_address,
                toDate(toStartOfDay(e.block_timestamp)) AS date,
                e.delta_amount_raw AS delta_amount_raw,
                e.delta_category AS delta_category,
                (toUInt64(toUnixTimestamp(e.block_timestamp)) * toUInt64(4294967296) + toUInt64(e.log_index)) AS event_order
            FROM `dbt`.`stg_pools__swapr_v3_events` e
            INNER JOIN pools p
                ON p.pool_address_no0x = e.pool_address
            WHERE e.block_timestamp < today()
              AND multiIf(
                  e.token_position = 'token0', p.token0_address,
                  e.token_position = 'token1', p.token1_address,
                  NULL
              ) IS NOT NULL
              
                
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(e.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_pools_swapr_v3_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(e.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_pools_swapr_v3_daily` AS x2
        WHERE 1=1 
      )
    
  

              
            ORDER BY pool_address_no0x, event_order
        ) sw
        ASOF LEFT JOIN (
            SELECT * FROM fee_events
            ORDER BY pool_address_no0x, event_order
        ) f
            ON sw.pool_address_no0x = f.pool_address_no0x
           AND sw.event_order >= f.event_order
        LEFT JOIN first_fee ff
            ON ff.pool_address_no0x = sw.pool_address_no0x
    )
    GROUP BY date, pool_address, token_address
),

overall_max_date AS (
    SELECT
        least(
            
                today(),
            
            yesterday(),
            (
                SELECT max(toDate(toStartOfDay(block_timestamp)))
                FROM `dbt`.`stg_pools__swapr_v3_events`
                
            )
        ) AS max_date
),


current_partition AS (
    SELECT
        max(date) AS max_date
    FROM `dbt`.`int_execution_pools_swapr_v3_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.pool_address,
        t1.token_address,
        t1.token_amount_raw AS balance_raw,
        t1.reserve_amount_raw AS reserve_raw,
        t1.fee_amount_raw AS fee_raw
    FROM `dbt`.`int_execution_pools_swapr_v3_daily` t1
    CROSS JOIN current_partition t2
    WHERE t1.date = t2.max_date
),



keys AS (
    SELECT DISTINCT
        pool_address,
        token_address
    FROM (
        SELECT pool_address, token_address FROM prev_balances
        UNION ALL
        SELECT pool_address, token_address FROM daily_deltas
    )
),

calendar AS (
    SELECT
        k.pool_address,
        k.token_address,
        
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
        c.date AS date,
        c.pool_address AS pool_address,
        c.token_address AS token_address,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.balance_raw, toInt256(0))
        
        AS balance_raw,
        sum(coalesce(d.daily_reserve_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.reserve_raw, toInt256(0))
        
        AS reserve_raw,
        sum(coalesce(d.daily_fee_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.pool_address, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.fee_raw, toInt256(0))
        
        AS fee_raw
    FROM calendar c
    LEFT JOIN daily_deltas d
        ON d.pool_address = c.pool_address
       AND d.token_address = c.token_address
       AND d.date = c.date
    
    LEFT JOIN prev_balances p
        ON p.pool_address = c.pool_address
       AND p.token_address = c.token_address
    
),

enriched AS (
    SELECT
        b.date AS date,
        'Swapr V3' AS protocol,
        b.pool_address AS pool_address,
        replaceAll(b.pool_address, '0x', '') AS pool_address_no0x,
        b.token_address AS token_address,
        tm.token AS token,
        b.balance_raw AS token_amount_raw,
        b.balance_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS token_amount,
        b.reserve_raw AS reserve_amount_raw,
        b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS reserve_amount,
        b.fee_raw AS fee_amount_raw,
        b.fee_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18)) AS fee_amount,
        pr.price AS price_usd,
        (b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))) * pr.price AS tvl_component_usd
    FROM balances b
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` tm
        ON tm.token_address = b.token_address
       AND b.date >= toDate(tm.date_start)
    ASOF LEFT JOIN (
        SELECT symbol, date, price FROM `dbt`.`int_execution_token_prices_daily` ORDER BY symbol, date
    ) pr
        ON pr.symbol = tm.token
       AND b.date >= pr.date
    WHERE b.balance_raw != 0
)

SELECT
    date,
    protocol,
    pool_address,
    pool_address_no0x,
    token_address,
    token,
    token_amount_raw,
    token_amount,
    reserve_amount_raw,
    reserve_amount,
    fee_amount_raw,
    fee_amount,
    price_usd,
    tvl_component_usd,
    (sum(tvl_component_usd) OVER (PARTITION BY date, pool_address) - tvl_component_usd)
        / nullIf(reserve_amount, 0) AS pool_implied_price_usd
FROM enriched