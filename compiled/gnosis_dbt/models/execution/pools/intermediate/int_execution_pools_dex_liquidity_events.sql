



WITH

all_liquidity AS (
    SELECT * FROM `dbt`.`stg_pools__dex_liquidity_uniswap_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_liquidity_swapr_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_liquidity_balancer_v2`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_liquidity_balancer_v3`
),

events_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.transaction_hash,
        l.log_index,
        l.protocol,
        l.pool_address,
        l.provider,
        l.event_type,
        l.token_address,
        tm.token                                                                 AS token_symbol,
        l.amount_raw,
        l.amount_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))         AS amount,
        l.tick_lower,
        l.tick_upper,
        l.liquidity_delta
    FROM all_liquidity l
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` tm
        ON  tm.token_address          = l.token_address
        AND toDate(l.block_timestamp) >= toDate(tm.date_start)
    WHERE l.amount_raw > 0
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(l.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_pools_dex_liquidity_events` AS x1
      WHERE 1=1 
    )
    AND toDate(l.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_pools_dex_liquidity_events` AS x2
      WHERE 1=1 
    )
  

      
),

tx_context AS (
    SELECT DISTINCT transaction_hash, from_address, to_address
    FROM `execution`.`transactions`
    WHERE transaction_hash IN (SELECT DISTINCT transaction_hash FROM events_base)
      
      AND block_timestamp >= (
          SELECT addDays(max(toDate(block_timestamp)), -3)
          FROM `dbt`.`int_execution_pools_dex_liquidity_events`
      )
      
),

with_meta AS (
    SELECT
        e.*,
        lower(tx.from_address) AS tx_from,
        lower(tx.to_address)   AS tx_to
    FROM events_base e
    LEFT JOIN tx_context tx ON tx.transaction_hash = e.transaction_hash
),

with_price AS (
    SELECT
        s.*,
        pr.price AS token_price_usd
    FROM with_meta s
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        ORDER BY symbol, date
    ) pr
        ON  pr.symbol                 = s.token_symbol
        AND toDate(s.block_timestamp) >= pr.date
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    provider,
    event_type,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount * token_price_usd AS amount_usd,
    tick_lower,
    tick_upper,
    liquidity_delta,
    tx_from,
    tx_to
FROM with_price
ORDER BY block_timestamp, transaction_hash, log_index, token_address