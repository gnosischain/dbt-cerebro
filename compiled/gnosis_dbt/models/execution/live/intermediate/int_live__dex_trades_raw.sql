WITH

all_swaps AS (
    SELECT * FROM `dbt`.`stg_live__dex_trades_uniswap_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_live__dex_trades_swapr_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_live__dex_trades_balancer_v2`
    UNION ALL
    SELECT * FROM `dbt`.`stg_live__dex_trades_balancer_v3`
),

normalized AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.transaction_hash,
        s.log_index,
        s.protocol,
        s.pool_address,
        s.token_bought_address,
        tb.token                                                               AS token_bought_symbol,
        s.amount_bought_raw,
        s.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))  AS amount_bought,
        s.token_sold_address,
        ts.token                                                               AS token_sold_symbol,
        s.amount_sold_raw,
        s.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))    AS amount_sold
    FROM all_swaps s
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` tb
        ON  tb.token_address = s.token_bought_address
        AND toDate(s.block_timestamp) >= toDate(tb.date_start)
    LEFT JOIN `dbt`.`stg_pools__tokens_meta` ts
        ON  ts.token_address = s.token_sold_address
        AND toDate(s.block_timestamp) >= toDate(ts.date_start)
    WHERE s.amount_bought_raw > 0
      AND s.amount_sold_raw   > 0
      
      AND s.block_timestamp >= (
          SELECT max(block_timestamp) FROM `execution_live`.`logs`
      ) - INTERVAL 48 HOUR
      
),

with_bought_price AS (
    SELECT
        n.*,
        pb.price AS token_bought_price_usd
    FROM normalized n
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        WHERE date >= today() - 7
        ORDER BY symbol, date
    ) pb
        ON  pb.symbol                 = n.token_bought_symbol
        AND toDate(n.block_timestamp) >= pb.date
),

with_sold_price AS (
    SELECT
        b.*,
        ps.price AS token_sold_price_usd
    FROM with_bought_price b
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        WHERE date >= today() - 7
        ORDER BY symbol, date
    ) ps
        ON  ps.symbol                 = b.token_sold_symbol
        AND toDate(b.block_timestamp) >= ps.date
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
    token_bought_price_usd,
    amount_bought * token_bought_price_usd                                   AS amount_bought_usd,
    token_sold_address,
    token_sold_symbol,
    amount_sold_raw,
    amount_sold,
    token_sold_price_usd,
    amount_sold * token_sold_price_usd                                       AS amount_sold_usd,
    CASE
        WHEN token_bought_price_usd IS NOT NULL AND token_sold_price_usd IS NOT NULL
            THEN least(
                amount_bought * token_bought_price_usd,
                amount_sold   * token_sold_price_usd
            )
        ELSE coalesce(
            amount_bought * token_bought_price_usd,
            amount_sold   * token_sold_price_usd
        )
    END                                                                      AS amount_usd
FROM with_sold_price