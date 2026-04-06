{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(protocol, pool_address)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'staging']
    )
}}

{#-
  Complete registry of Uniswap V3 and Swapr V3 (Algebra) pools on Gnosis Chain.
  Combines factory creation events with pool Initialize events to produce
  a single source of truth for pool metadata.

  Columns:
    - protocol, pool_address, pool_address_no0x
    - token0_address, token1_address
    - fee_tier_ppm: static fee for UniV3 (from PoolCreated); NULL for Swapr (dynamic fees)
    - tick_spacing
    - init_tick, init_sqrt_price_x96: from the Initialize event (NULL if pool never initialized)
    - created_at: block_timestamp of the factory creation event
-#}

WITH

uniswap_v3_created AS (
    SELECT
        'Uniswap V3' AS protocol,
        lower(decoded_params['pool']) AS pool_address,
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        toUInt32OrNull(decoded_params['fee']) AS fee_tier_ppm,
        toInt32OrNull(decoded_params['tickSpacing']) AS tick_spacing,
        block_timestamp AS created_at
    FROM {{ ref('contracts_UniswapV3_Factory_events') }}
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

swapr_v3_created AS (
    SELECT
        'Swapr V3' AS protocol,
        lower(decoded_params['pool']) AS pool_address,
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        CAST(NULL AS Nullable(UInt32)) AS fee_tier_ppm,
        CAST(NULL AS Nullable(Int32)) AS tick_spacing,
        block_timestamp AS created_at
    FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }}
    WHERE event_name = 'Pool'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

all_pools AS (
    SELECT * FROM uniswap_v3_created
    UNION ALL
    SELECT * FROM swapr_v3_created
),

{#- Swapr V3 pools can have their tick spacing changed via TickSpacing events.
    Take the latest tick spacing per pool. -#}
swapr_v3_tick_spacing AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        argMax(toInt32OrNull(decoded_params['newTickSpacing']), block_timestamp) AS tick_spacing
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name = 'TickSpacing'
      AND decoded_params['newTickSpacing'] IS NOT NULL
    GROUP BY pool_address_no0x
),

{#- Initialize events from pool contracts give the initial price and tick. -#}
uniswap_v3_init AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        decoded_params['sqrtPriceX96'] AS init_sqrt_price_x96,
        toInt32OrNull(decoded_params['tick']) AS init_tick
    FROM {{ ref('contracts_UniswapV3_Pool_events') }}
    WHERE event_name = 'Initialize'
      AND decoded_params['sqrtPriceX96'] IS NOT NULL
      AND decoded_params['tick'] IS NOT NULL
),

swapr_v3_init AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address_no0x,
        decoded_params['price'] AS init_sqrt_price_x96,
        toInt32OrNull(decoded_params['tick']) AS init_tick
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name = 'Initialize'
      AND decoded_params['price'] IS NOT NULL
      AND decoded_params['tick'] IS NOT NULL
),

all_inits AS (
    SELECT * FROM uniswap_v3_init
    UNION ALL
    SELECT * FROM swapr_v3_init
)

SELECT
    p.protocol                                AS protocol,
    p.pool_address                            AS pool_address,
    p.pool_address_no0x                       AS pool_address_no0x,
    p.token0_address                          AS token0_address,
    p.token1_address                          AS token1_address,
    p.fee_tier_ppm                            AS fee_tier_ppm,
    coalesce(ts.tick_spacing, p.tick_spacing) AS tick_spacing,
    i.init_tick                               AS init_tick,
    i.init_sqrt_price_x96                     AS init_sqrt_price_x96,
    p.created_at                              AS created_at
FROM all_pools p
LEFT JOIN all_inits i
    ON i.pool_address_no0x = p.pool_address_no0x
LEFT JOIN swapr_v3_tick_spacing ts
    ON ts.pool_address_no0x = p.pool_address_no0x
    AND p.protocol = 'Swapr V3'
