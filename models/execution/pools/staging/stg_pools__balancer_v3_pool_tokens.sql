{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(pool_address, token_index)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balancer_v3', 'staging']
    )
}}

{#-
  Ordered token list per Balancer V3 pool, derived from Swap events.

  Balancer V3 stores pool tokens in ascending address order at creation time.
  ROW_NUMBER() over sorted unique token addresses reproduces this ordering,
  giving the same 0-based index used in LiquidityAdded/LiquidityRemoved
  amountsAddedRaw/amountsRemovedRaw arrays.

  Tokens are the ERC4626 wrapper tokens that the pool actually holds
  (e.g. waGnoGNO, waGnoWETH). Resolve to underlying via
  stg_pools__balancer_v3_token_map for price lookups.

  Limitation: only pools that have seen at least one Swap event appear.
  Pools with deposited liquidity but zero swaps are excluded — acceptable
  in practice since such pools have negligible activity.
-#}

WITH unique_pool_tokens AS (
    SELECT DISTINCT
        pool_address,
        token_address
    FROM {{ ref('stg_pools__balancer_v3_events') }}
    WHERE event_type = 'Swap'
      AND token_address IS NOT NULL
      AND token_address != ''
)

SELECT
    pool_address,
    token_address,
    toUInt64(ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY token_address) - 1) AS token_index
FROM unique_pool_tokens