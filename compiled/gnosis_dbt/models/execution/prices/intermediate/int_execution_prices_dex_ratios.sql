

-- Daily DEX-derived USD prices for whitelist tokens that have NO Gnosis Chainlink
-- feed: GBPe, BRLA, BRZ, COW, SAFE, sGNO. For each trade where the target token is
-- swapped against an oracle-priced "anchor" token, the target's implied USD price is
-- (anchor_amount * anchor_usd) / target_amount. We take the daily median over such
-- trades, requiring a >= $1000 notional and >= 5 trades/day (Dune's guardrails).
--
-- Sources are strictly UNPRICED to avoid a dependency cycle with the price hub:
--   * int_execution_pools_dex_trades_raw (Uniswap V3 / Swapr V3 / Balancer V2/V3)
--   * stg_cow__trades  (CoW; decimals/symbols joined from tokens_meta here, NOT the
--     priced int_execution_cow_trades, which refs the hub and would cycle).
-- The USD anchor is the native oracle feed int_execution_prices_oracle_daily.
-- BRZ<-BRLA fallback is applied downstream in the assembly model, not here.




WITH unpriced_trades AS (

    -- Pools: already decimal-adjusted with resolved symbols.
    SELECT
        toDate(block_timestamp)     AS date,
        token_bought_symbol         AS sym_bought,
        amount_bought               AS amt_bought,
        token_sold_symbol           AS sym_sold,
        amount_sold                 AS amt_sold
    FROM `dbt`.`int_execution_pools_dex_trades_raw`
    WHERE amount_bought > 0
      AND amount_sold   > 0
      AND block_timestamp < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_prices_dex_ratios` AS x1
        WHERE 1=1 
      )
    
  

      

    UNION ALL

    -- CoW: unpriced; resolve symbol + decimals from tokens_meta (no hub dependency).
    SELECT
        toDate(t.block_timestamp)                                                       AS date,
        tb.token                                                                        AS sym_bought,
        t.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))           AS amt_bought,
        ts.token                                                                        AS sym_sold,
        t.amount_sold_raw   / POWER(10, if(ts.decimals > 0, ts.decimals, 18))           AS amt_sold
    FROM `dbt`.`stg_cow__trades` t
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` tb
        ON  tb.token_address = t.token_bought_address
        AND toDate(t.block_timestamp) >= toDate(tb.date_start)
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` ts
        ON  ts.token_address = t.token_sold_address
        AND toDate(t.block_timestamp) >= toDate(ts.date_start)
    WHERE t.amount_bought_raw > 0
      AND t.amount_sold_raw   > 0
      AND t.block_timestamp < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_prices_dex_ratios` AS x1
        WHERE 1=1 
      )
    
  

      
),

anchor AS (
    SELECT upper(symbol) AS sym_upper, date, price
    FROM `dbt`.`int_execution_prices_oracle_daily`
    WHERE price > 0
),

legs AS (

    -- target token was SOLD, an anchor token was BOUGHT
    SELECT
        t.date,
        t.sym_sold                                          AS symbol,
        t.amt_bought * a.price / nullIf(t.amt_sold, 0)      AS implied_usd,
        t.amt_bought * a.price                              AS trade_usd
    FROM unpriced_trades t
    INNER JOIN anchor a
        ON a.sym_upper = upper(t.sym_bought)
       AND a.date      = t.date
    WHERE upper(t.sym_sold) IN ('GBPE','BRLA','BRZ','COW','SAFE','SGNO')

    UNION ALL

    -- target token was BOUGHT, an anchor token was SOLD
    SELECT
        t.date,
        t.sym_bought                                        AS symbol,
        t.amt_sold * a.price / nullIf(t.amt_bought, 0)      AS implied_usd,
        t.amt_sold * a.price                                AS trade_usd
    FROM unpriced_trades t
    INNER JOIN anchor a
        ON a.sym_upper = upper(t.sym_sold)
       AND a.date      = t.date
    WHERE upper(t.sym_bought) IN ('GBPE','BRLA','BRZ','COW','SAFE','SGNO')
)

SELECT
    symbol,
    date,
    quantileExact(0.5)(implied_usd)     AS price,
    count()                             AS n_trades
FROM legs
WHERE trade_usd  >= 1000
  AND implied_usd > 0
GROUP BY symbol, date
HAVING count() >= 5