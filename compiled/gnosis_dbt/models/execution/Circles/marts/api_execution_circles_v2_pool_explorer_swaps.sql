
-- Individual recent swaps per main Circles DEX pool (scoped, for the Pool Explorer swaps table).
WITH p AS ( SELECT lower(pool_address) AS pool_address FROM `dbt`.`circles_liquidity_pools` )
SELECT
    lower(t.pool_address)      AS pool_address,
    t.block_timestamp          AS ts,
    t.transaction_hash         AS tx_hash,
    t.token_sold_symbol        AS token_sold,
    t.amount_sold              AS amount_sold,
    t.token_bought_symbol      AS token_bought,
    t.amount_bought            AS amount_bought,
    t.amount_usd               AS amount_usd,
    coalesce(t.taker, t.tx_from) AS trader
FROM `dbt`.`int_execution_pools_dex_trades` t
INNER JOIN p ON p.pool_address = lower(t.pool_address)
WHERE t.block_timestamp < today()