
-- Daily swap activity per main Circles DEX pool (scoped by pool_address in the Pool Explorer).
WITH p AS ( SELECT lower(pool_address) AS pool_address FROM `dbt`.`circles_liquidity_pools` )
SELECT
    toDate(t.block_timestamp)                 AS date,
    lower(t.pool_address)                     AS pool_address,
    count()                                   AS n_swaps,
    sum(t.amount_usd)                         AS volume_usd,
    uniqExact(coalesce(t.taker, t.tx_from))   AS n_traders
FROM `dbt`.`int_execution_pools_dex_trades` t
INNER JOIN p ON p.pool_address = lower(t.pool_address)
WHERE toDate(t.block_timestamp) < today()
GROUP BY date, pool_address