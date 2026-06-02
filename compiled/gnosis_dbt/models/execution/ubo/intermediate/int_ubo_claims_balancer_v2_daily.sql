

-- Per-user UBO supply claims for Balancer V2.
--
-- Balancer V2 uses a single Vault contract that custodies ALL pool tokens.
-- Each LP's net token position is the cumulative sum of their PoolBalanceChanged
-- deltas across all pools for that token. container_address is therefore the
-- Vault address for every row; token_address is the canonical address per
-- tokens_whitelist on that date.
--
-- Tracking is keyed by (ubo_address, symbol) through the cumsum so that token
-- migrations (e.g. EURe V1 → V2) collapse into a single continuous series.
-- The canonical token_address is resolved at the final SELECT via tokens_whitelist.




WITH

daily_deltas AS (
    SELECT
        toDate(liq.block_timestamp)     AS date,
        lower(liq.provider)             AS ubo_address,
        tw.symbol                       AS symbol,
        sum(if(liq.event_type = 'mint',
                toInt256(liq.amount_raw),
               -toInt256(liq.amount_raw))) AS daily_delta_raw
    FROM `dbt`.`stg_pools__dex_liquidity_balancer_v2` liq
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON  lower(tw.address)           = lower(liq.token_address)
        AND toDate(liq.block_timestamp) >= tw.date_start
        AND (tw.date_end IS NULL OR toDate(liq.block_timestamp) < tw.date_end)
    WHERE liq.block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(liq.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_ubo_claims_balancer_v2_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(liq.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_ubo_claims_balancer_v2_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY date, ubo_address, symbol
),

overall_max_date AS (
    SELECT
        
            yesterday()
         AS max_date
),


current_partition AS (
    SELECT max(date) AS max_date
    FROM `dbt`.`int_ubo_claims_balancer_v2_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.balance_raw
    FROM `dbt`.`int_ubo_claims_balancer_v2_daily` t1
    CROSS JOIN current_partition t2
    INNER JOIN `dbt`.`tokens_whitelist` tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = t2.max_date
),



keys AS (
    SELECT DISTINCT ubo_address, symbol
    FROM (
        SELECT ubo_address, symbol FROM prev_balances
        UNION ALL
        SELECT ubo_address, symbol FROM daily_deltas
    )
),

calendar AS (
    SELECT
        k.ubo_address,
        k.symbol,
        
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
        c.date        AS date,
        c.ubo_address AS ubo_address,
        c.symbol      AS symbol,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.ubo_address, c.symbol
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.balance_raw, toInt256(0))
        
        AS balance_raw
    FROM calendar c
    LEFT JOIN daily_deltas d
        ON  d.ubo_address = c.ubo_address
        AND d.symbol      = c.symbol
        AND d.date        = c.date
    
    LEFT JOIN prev_balances p
        ON  p.ubo_address = c.ubo_address
        AND p.symbol      = c.symbol
    
)

SELECT
    b.date                                                                  AS date,
    'Balancer V2'                                                           AS protocol,
    lower('0xba12222222228d8ba445958a75a0704d566bf2c8')                    AS container_address,
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