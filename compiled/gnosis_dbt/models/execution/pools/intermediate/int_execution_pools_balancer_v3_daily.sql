



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
        count() AS token_cnt,
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
                -- PRIMARY: the address-sorted swap-token list (arraySort of Swap tokenIn/tokenOut)
                -- fully covers the pool -> use it. Balancer V3 registers tokens address-sorted, so
                -- this equals the Vault order that indexes the positional amountsAddedRaw/RemovedRaw
                -- arrays in Liquidity events. The decoded PoolRegistered tokenConfig struct-array is
                -- unreliable (inner tokens of multi-token pools decode to 0x0), so it is only a fallback.
                length(ifNull(s.swap_tokens, [])) = st.token_cnt,
                s.swap_tokens[toInt32(c.token_index) + 1],
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

-- Aggregate (protocol + pool-creator) swap-fee % per pool over time. Balancer V3 skims this
-- portion of every swap fee OUT of the pool's balancesRaw at swap time (the LP portion stays),
-- so the reserve must subtract it or it accumulates a "ghost" balance (== all protocol fees ever
-- skimmed) that dominates once a high-throughput pool drains. The % is time-varying
-- (AggregateSwapFeePercentageChanged); a swap with no prior change event falls back to the 50%
-- pre-2026-04 global default. CAVEAT: this recovers only the aggregate SWAP fee. Balancer also
-- skims an aggregate YIELD fee on rate-bearing tokens (sDAI/GNO/wstETH...), which has NO
-- per-accrual event and is not reconstructable here -- it remains a known positive residual vs
-- on-chain balancesRaw for rate pools (full fix: periodic on-chain getTokenInfo reconciliation).
agg_swap_fee AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        block_timestamp AS ts,
        toInt256OrNull(decoded_params['aggregateSwapFeePercentage']) AS agg_pct_raw
    FROM `dbt`.`contracts_BalancerV3_Vault_events`
    WHERE event_name = 'AggregateSwapFeePercentageChanged'
      AND toInt256OrNull(decoded_params['aggregateSwapFeePercentage']) IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        pool_address,
        toDateTime64('2020-01-01 00:00:00', 0, 'UTC') AS ts,
        toInt256(500000000000000000) AS agg_pct_raw
    FROM `dbt`.`stg_pools__balancer_v3_events`
    WHERE event_type = 'Swap' AND pool_address IS NOT NULL
),

deltas_pool AS (
    SELECT
        e.block_timestamp AS block_timestamp,
        p.pool_address AS pool_address,
        p.token_address AS token_address,
        e.delta_amount_raw AS delta_amount_raw,
        e.fee_amount_raw AS fee_amount_raw,
        -- liquidity add/remove carries no swap fee: reserve delta == physical delta
        e.delta_amount_raw AS reserve_delta_raw
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
        e.fee_amount_raw AS fee_amount_raw,
        -- net out the aggregate (protocol+creator) swap fee, which exits the pool's balancesRaw;
        -- a.agg_pct_raw is the as-of aggregate % for this swap (ASOF, scaled 1e18). Integer math
        -- keeps Int256 precision. coalesce->0 is a safe no-subtraction fallback (never hit: every
        -- swap pool has a default 50% row in agg_swap_fee).
        e.delta_amount_raw
          - intDiv(e.fee_amount_raw * coalesce(a.agg_pct_raw, toInt256(0)), toInt256(1000000000000000000))
          AS reserve_delta_raw
    FROM `dbt`.`stg_pools__balancer_v3_events` e
    ASOF LEFT JOIN agg_swap_fee a
        ON a.pool_address = e.pool_address
       AND a.ts <= e.block_timestamp
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
        -- Both balance and reserve use reserve_delta_raw (== delta minus the aggregate swap fee
        -- that exits balancesRaw). The gross sum(delta_amount_raw) is NOT the pool balance: the
        -- skimmed aggregate fees are never returned via Swap/Liquidity events (they leave on a
        -- separate protocol-fee-collection call we don't track), so the gross sum accumulates a
        -- ghost. reserve == token_amount == on-chain balancesRaw (net of aggregate SWAP fee; the
        -- aggregate YIELD fee on rate tokens is a known un-subtracted residual). Gross swap fees
        -- stay in int_execution_pools_fees_daily.
        sum(reserve_delta_raw) AS daily_delta_raw,
        sum(reserve_delta_raw) AS daily_reserve_delta_raw,
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
    -- Carry-forward frontier = the EARLIEST per-(pool,token) last date, NOT the
    -- single global max(date). A thin / sporadically-traded pool that skips a day
    -- used to fall off the global frontier: it dropped out of prev_balances, the
    -- calendar only ever generated dates from the global frontier (never its own),
    -- so it accreted permanent gaps and only re-materialised a stray day when it
    -- happened to trade inside a run's window (observed: 0x155c… s-gCRC/sDAI at
    -- 5/48 days; density tracked trade frequency across all thin pools).
    -- Anchoring the window at the earliest per-pool frontier regenerates EVERY pool
    -- densely from a date they all share, so behind pools are re-densified together
    -- with the rest and none can drop. In steady state all pools share one frontier,
    -- so this stays a 1-day window. Strategy is delete+insert (not insert_overwrite):
    -- the frontier calendar emits only the new day(s), and delete+insert removes just
    -- those (date,pool,token) unique_key rows before re-inserting -- so the rest of the
    -- month partition is untouched. insert_overwrite would REPLACE the whole month
    -- partition with only the emitted day(s) and wipe the earlier days (the frontier
    -- calendar does not regenerate a full month), so it must NOT be used on this model.
    -- Deep historical gaps: rebuild with --full-refresh (per-pool non-incremental
    -- calendar below).
    SELECT min(pool_max) AS max_date
    FROM (
        SELECT max(date) AS pool_max
        FROM `dbt`.`int_execution_pools_balancer_v3_daily`
        WHERE date < yesterday()
        GROUP BY pool_address, token_address
    )
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