{{ config(materialized='view', tags=['production','execution','circles_v2','api:circles_v2_pool_explorer_liquidity','granularity:daily','tier1']) }}
-- Daily liquidity events (Mint = add, Burn = remove) per Uniswap V3 Circles pool.
-- One event emits two token rows in the staging view, so events are deduped on (tx, log_index).
WITH p AS (
    SELECT replaceAll(lower(pool_address), '0x', '') AS pool_no0x, lower(pool_address) AS pool_address
    FROM {{ ref('circles_liquidity_pools') }} WHERE protocol = 'Uniswap V3'
)
SELECT
    toDate(e.block_timestamp)                              AS date,
    p.pool_address                                        AS pool_address,
    multiIf(e.event_type = 'Mint', 'Add', 'Remove')       AS event_kind,
    uniqExact((e.transaction_hash, e.log_index))          AS n_events
FROM {{ ref('stg_pools__uniswap_v3_events') }} e
INNER JOIN p ON p.pool_no0x = e.pool_address
WHERE e.event_type IN ('Mint', 'Burn') AND e.block_timestamp < today()
GROUP BY date, pool_address, event_kind
