

-- Daily distinct traders per main Circles DEX pool (seed circles_liquidity_pools).
-- A "trader" is the swap taker (Swap event recipient), falling back to the tx signer.
WITH p AS (
    SELECT lower(pool_address) AS pool_address, label FROM `dbt`.`circles_liquidity_pools`
)
SELECT
    toDate(t.block_timestamp)                 AS date,
    p.label                                   AS pool,
    lower(t.pool_address)                     AS pool_address,
    uniqExact(coalesce(t.taker, t.tx_from))   AS distinct_traders,
    count()                                   AS trades
FROM `dbt`.`int_execution_pools_dex_trades` t
INNER JOIN p ON p.pool_address = lower(t.pool_address)
WHERE toDate(t.block_timestamp) < today()
GROUP BY date, pool, pool_address