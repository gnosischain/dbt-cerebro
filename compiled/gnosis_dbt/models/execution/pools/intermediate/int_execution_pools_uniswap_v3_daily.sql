




WITH

pools AS (
    SELECT
        pool_address,
        replaceAll(pool_address, '0x', '') AS pool_address_no0x,
        token0_address,
        token1_address,
        fee_tier_ppm
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Uniswap V3'
      AND pool_address IN (
          SELECT lower(address)
          FROM `dbt`.`contracts_whitelist`
          WHERE contract_type = 'UniswapV3Pool'
      )
),

daily_deltas AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS date,
        concat('0x', e.pool_address) AS pool_address,
        multiIf(
            e.token_position = 'token0', p.token0_address,
            e.token_position = 'token1', p.token1_address,
            NULL
        ) AS token_address,
        sum(e.delta_amount_raw) AS daily_delta_raw,
        sum(multiIf(
            e.delta_category = 'swap_in',
                e.delta_amount_raw - intDiv(e.delta_amount_raw * toInt256(p.fee_tier_ppm), toInt256(1000000)),
            e.delta_category IN ('fee_collection', 'flash_fee'),
                toInt256(0),
            e.delta_amount_raw
        )) AS daily_reserve_delta_raw,
        sum(multiIf(
            e.delta_category = 'swap_in',
                intDiv(e.delta_amount_raw * toInt256(p.fee_tier_ppm), toInt256(1000000)),
            e.delta_category IN ('fee_collection', 'flash_fee'),
                e.delta_amount_raw,
            toInt256(0)
        )) AS daily_fee_delta_raw
    FROM `dbt`.`stg_pools__uniswap_v3_events` e
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
      FROM `dbt`.`int_execution_pools_uniswap_v3_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(e.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_pools_uniswap_v3_daily` AS x2
      WHERE 1=1 
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
                FROM `dbt`.`stg_pools__uniswap_v3_events`
                
            )
        ) AS max_date
),


current_partition AS (
    SELECT
        max(date) AS max_date
    FROM `dbt`.`int_execution_pools_uniswap_v3_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.pool_address,
        t1.token_address,
        t1.token_amount_raw AS balance_raw,
        t1.reserve_amount_raw AS reserve_raw,
        t1.fee_amount_raw AS fee_raw
    FROM `dbt`.`int_execution_pools_uniswap_v3_daily` t1
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
        'Uniswap V3' AS protocol,
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