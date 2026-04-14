


WITH

swaps AS (
    SELECT
        s.*,
        tx.tx_from,
        tx.tx_to
    FROM (
        SELECT *
        FROM `dbt`.`int_execution_pools_dex_trades_raw`
        
          
  
    
    

   WHERE 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_pools_dex_trades` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_pools_dex_trades` AS x2
      WHERE 1=1 
    )
  

        
    ) s
    LEFT JOIN (
        SELECT transaction_hash, tx_from, tx_to
        FROM `dbt`.`int_execution_pools_dex_trades_tx_context`
        
        WHERE block_timestamp >= (SELECT addDays(max(toDate(block_timestamp)), -3) FROM `dbt`.`int_execution_pools_dex_trades`)
        
    ) tx ON tx.transaction_hash = s.transaction_hash
),

with_bought_price AS (
    SELECT
        s.*,
        pb.price AS token_bought_price_usd
    FROM swaps s
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        
        WHERE date >= (SELECT addDays(max(toDate(block_timestamp)), -30) FROM `dbt`.`int_execution_pools_dex_trades`)
        
        ORDER BY symbol, date
    ) pb
        ON  pb.symbol                 = s.token_bought_symbol
        AND toDate(s.block_timestamp) >= pb.date
),

with_sold_price AS (
    SELECT
        s.*,
        ps.price AS token_sold_price_usd
    FROM with_bought_price s
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        
        WHERE date >= (SELECT addDays(max(toDate(block_timestamp)), -30) FROM `dbt`.`int_execution_pools_dex_trades`)
        
        ORDER BY symbol, date
    ) ps
        ON  ps.symbol                 = s.token_sold_symbol
        AND toDate(s.block_timestamp) >= ps.date
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    token_bought_address,
    token_bought_symbol,
    amount_bought_raw,
    amount_bought,
    token_sold_address,
    token_sold_symbol,
    amount_sold_raw,
    amount_sold,
    COALESCE(
        amount_bought * token_bought_price_usd,
        amount_sold   * token_sold_price_usd
    )                   AS amount_usd,
    coalesce(taker, tx_from) AS taker,
    tx_from,
    tx_to
FROM with_sold_price