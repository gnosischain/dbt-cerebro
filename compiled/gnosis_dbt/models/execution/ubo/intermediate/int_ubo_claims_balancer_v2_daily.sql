

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
      
        
  

      
    GROUP BY date, ubo_address, symbol
),

overall_max_date AS (
    SELECT
        
            yesterday()
         AS max_date
),




calendar AS (
    SELECT
        ubo_address,
        symbol,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.ubo_address,
            d.symbol,
            min(d.date)                                   AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM daily_deltas d
        CROSS JOIN overall_max_date o
        GROUP BY d.ubo_address, d.symbol
    )
    ARRAY JOIN range(num_days + 1) AS offset
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
        
        AS balance_raw
    FROM calendar c
    LEFT JOIN daily_deltas d
        ON  d.ubo_address = c.ubo_address
        AND d.symbol      = c.symbol
        AND d.date        = c.date
    
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