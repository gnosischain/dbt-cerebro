{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

-- Maps every CRC20 wrapper to the DEX pool(s) it trades in, covering:
--   - Uniswap V3 / Swapr V3: discovered from stg_pools__v3_pool_registry
--     (requires the pool address in contracts_whitelist.csv with type UniswapV3Pool)
--   - Balancer V2: captured automatically via the single Balancer vault contract
--     (all swaps go through contracts_BalancerV2_Vault_events, no whitelist needed)
-- Used as a reference / filter helper by downstream price models.

-- Uniswap V3 / Swapr V3 — token0 is CRC20
SELECT
    r.pool_address,
    r.protocol,
    w.wrapper_address   AS crc20_token,
    r.token1_address    AS backing_token,
    true                AS crc20_is_token0,
    w.avatar,
    w.circles_type,
    wt.symbol           AS crc20_symbol
FROM {{ ref('stg_pools__v3_pool_registry') }} r
INNER JOIN {{ ref('int_execution_circles_v2_wrappers') }} w
    ON r.token0_address = w.wrapper_address
LEFT JOIN {{ ref('int_execution_circles_v2_wrapper_tokens') }} wt
    ON wt.wrapper_address = w.wrapper_address

UNION ALL

-- Uniswap V3 / Swapr V3 — token1 is CRC20
SELECT
    r.pool_address,
    r.protocol,
    w.wrapper_address   AS crc20_token,
    r.token0_address    AS backing_token,
    false               AS crc20_is_token0,
    w.avatar,
    w.circles_type,
    wt.symbol           AS crc20_symbol
FROM {{ ref('stg_pools__v3_pool_registry') }} r
INNER JOIN {{ ref('int_execution_circles_v2_wrappers') }} w
    ON r.token1_address = w.wrapper_address
LEFT JOIN {{ ref('int_execution_circles_v2_wrapper_tokens') }} wt
    ON wt.wrapper_address = w.wrapper_address
