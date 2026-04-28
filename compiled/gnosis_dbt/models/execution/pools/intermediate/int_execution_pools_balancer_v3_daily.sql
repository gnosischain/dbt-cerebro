



WITH

/* ========================================
   Token config resolution for Balancer V3
   ======================================== */

swap_tokens AS (
    SELECT
        pool_address,
        arraySort(groupUniqArray(token_address)) AS swap_tokens
    FROM `dbt`.`stg_pools__balancer_v3_events`
    WHERE event_type = 'Swap'
      AND pool_address IS NOT NULL
      AND token_address IS NOT NULL
    GROUP BY pool_address
),

tokenconfig_raw AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        token_idx AS token_index,
        lower(concat('0x', right(replaceAll(replaceAll(token_val, '"', ''), '0x', ''), 40))) AS token_address,
        token_address IN (
            '0x0000000000000000000000000000000000000000',
            '0x0000000000000000000000000000000000000001'
        ) AS is_sentinel,
        block_timestamp,
        log_index
    FROM `dbt`.`contracts_BalancerV3_Vault_events`
    ARRAY JOIN
        range(length(JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')))) AS token_idx,
        JSONExtractArrayRaw(ifNull(decoded_params['tokenConfig'], '[]')) AS token_val
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['tokenConfig'] IS NOT NULL
),

tokenconfig AS (
    SELECT
        pool_address,
        token_index,
        token_address,
        is_sentinel
    FROM (
        SELECT
            pool_address,
            token_index,
            token_address,
            is_sentinel,
            row_number() OVER (
                PARTITION BY pool_address, token_index
                ORDER BY block_timestamp DESC, log_index DESC
            ) AS rn
        FROM tokenconfig_raw
    )
    WHERE rn = 1
),

tokenconfig_stats AS (
    SELECT
        pool_address,
        countIf(not is_sentinel) AS valid_cnt,
        anyIf(token_address, not is_sentinel) AS any_valid_token
    FROM tokenconfig
    GROUP BY pool_address
),

pool_tokens AS (
    SELECT
        pool_address,
        token_index,
        token_address
    FROM (
        SELECT
            c.pool_address AS pool_address,
            c.token_index AS token_index,
            multiIf(
                not c.is_sentinel,
                c.token_address,
                length(ifNull(s.swap_tokens, [])) = 2 AND st.valid_cnt = 1,
                if(st.any_valid_token = s.swap_tokens[1], s.swap_tokens[2], s.swap_tokens[1]),
                length(ifNull(s.swap_tokens, [])) = 2 AND st.valid_cnt = 0,
                s.swap_tokens[toInt32(c.token_index) + 1],
                NULL
            ) AS token_address
        FROM tokenconfig c
        LEFT JOIN swap_tokens s
            ON s.pool_address = c.pool_address
        LEFT JOIN tokenconfig_stats st
            ON st.pool_address = c.pool_address
    )
    WHERE token_address IS NOT NULL
),

/* ========================================
   Delta events
   ======================================== */

deltas_pool AS (
    SELECT
        e.block_timestamp AS block_timestamp,
        p.pool_address AS pool_address,
        p.token_address AS token_address,
        e.delta_amount_raw AS delta_amount_raw,
        e.fee_amount_raw AS fee_amount_raw
    FROM `dbt`.`stg_pools__balancer_v3_events` e
    INNER JOIN pool_tokens p
        ON e.pool_address = p.pool_address
       AND e.token_index = p.token_index
    WHERE e.delta_amount_raw IS NOT NULL
      AND e.token_index IS NOT NULL
      AND e.pool_address IS NOT NULL
),

deltas_swap AS (
    SELECT
        e.block_timestamp AS block_timestamp,
        e.pool_address AS pool_address,
        lower(e.token_address) AS token_address,
        e.delta_amount_raw AS delta_amount_raw,
        e.fee_amount_raw AS fee_amount_raw
    FROM `dbt`.`stg_pools__balancer_v3_events` e
    WHERE e.event_type = 'Swap'
      AND e.delta_amount_raw IS NOT NULL
      AND e.token_address IS NOT NULL
      AND e.pool_address IS NOT NULL
),

all_deltas AS (
    SELECT * FROM deltas_pool
    UNION ALL
    SELECT * FROM deltas_swap
),

daily_deltas AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        concat('0x', pool_address) AS pool_address,
        token_address,
        sum(delta_amount_raw) AS daily_delta_raw,
        sum(delta_amount_raw - fee_amount_raw) AS daily_reserve_delta_raw,
        sum(fee_amount_raw) AS daily_fee_delta_raw
    FROM all_deltas
    WHERE block_timestamp < today()
      AND lower(token_address) NOT IN (
          '0x0000000000000000000000000000000000000000',
          '0x0000000000000000000000000000000000000001'
      )
      AND lower(token_address) != concat('0x', pool_address)
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_pools_balancer_v3_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_pools_balancer_v3_daily` AS x2
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
                FROM `dbt`.`stg_pools__balancer_v3_events`
                
            )
        ) AS max_date
),


current_partition AS (
    SELECT
        max(date) AS max_date
    FROM `dbt`.`int_execution_pools_balancer_v3_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.pool_address,
        t1.token_address,
        t1.token_amount_raw AS balance_raw,
        t1.reserve_amount_raw AS reserve_raw,
        t1.fee_amount_raw AS fee_raw
    FROM `dbt`.`int_execution_pools_balancer_v3_daily` t1
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
        'Balancer V3' AS protocol,
        b.pool_address AS pool_address,
        replaceAll(b.pool_address, '0x', '') AS pool_address_no0x,
        lower(b.token_address) AS token_address,
        tm.token AS token,
        b.balance_raw AS token_amount_raw,
        b.balance_raw / POWER(10, if(tm.decimals > 0, tm.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS token_amount,
        b.reserve_raw AS reserve_amount_raw,
        b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS reserve_amount,
        b.fee_raw AS fee_amount_raw,
        b.fee_raw / POWER(10, if(tm.decimals > 0, tm.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18))) AS fee_amount,
        pr.price AS price_usd,
        (b.reserve_raw / POWER(10, if(tm.decimals > 0, tm.decimals, if(wm.wrapper_decimals > 0, wm.wrapper_decimals, 18)))) * pr.price AS tvl_component_usd
    FROM balances b
    LEFT JOIN `dbt`.`stg_pools__balancer_v3_token_map` wm
        ON wm.wrapper_address = lower(b.token_address)
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` tm
        ON tm.token_address = coalesce(nullIf(wm.underlying_address, ''), lower(b.token_address))
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