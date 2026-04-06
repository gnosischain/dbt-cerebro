{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index, token_address)',
        unique_key='(block_timestamp, transaction_hash, log_index, token_address)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET join_use_nulls = 0",
            "SET allow_experimental_json_type = 1"
        ],
        tags=['production', 'execution', 'pools', 'liquidity', 'intermediate']
    )
}}

{#-
  Individual LP add/remove events across all protocols on Gnosis Chain.
  One row per (event, token) — deliberately not pivoted to wide format
  because Balancer pools can hold 2–8 tokens, making a fixed-width
  schema incorrect. Consumers aggregate per event when needed.

  Protocols:
    - Uniswap V3  : Mint / Burn events; provider = decoded_params['owner']
    - Swapr V3    : same (Algebra fork)
    - Balancer V2 : PoolBalanceChanged; provider = decoded_params['liquidityProvider']

  Balancer V3 covered via two-step token resolution:
    1. stg_pools__balancer_v3_pool_tokens: (pool, token_index) → wrapper address
       Built from Swap events; Balancer V3 sorts tokens ascending by address.
    2. stg_pools__balancer_v3_token_map: wrapper address → underlying address
       Used so the output token_address is the priced underlying (GNO, WETH, etc.)
       rather than the unwieldy waGno* wrapper that lacks its own price entry.

  event_type values: 'mint' | 'burn'
  amount_raw is always positive; event_type conveys direction.

  Batching:
    When called with start_month/end_month vars (e.g. via the full-refresh script),
    each protocol CTE filters to that month window. Otherwise falls back to the
    incremental filter based on max(block_timestamp) in the existing table.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

-- =============================================
-- Balancer V2: pool_id → pool_address lookup
-- Not incrementally filtered (same reason as dex_trades)
-- =============================================
balancer_v2_pool_registry AS (
    SELECT DISTINCT
        lower(decoded_params['poolId'])                                          AS pool_id,
        concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
    FROM {{ ref('contracts_BalancerV2_Vault_events') }}
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['poolId']      IS NOT NULL
      AND decoded_params['poolAddress'] IS NOT NULL
),

-- =============================================
-- Uniswap V3 — Mint / Burn
-- One row per token per event (token0 and token1 each become a row)
-- =============================================
uniswap_v3_liquidity AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Uniswap V3'                                    AS protocol,
        r.pool_address                                  AS pool_address,
        lower(decoded_params['owner'])                  AS provider,
        multiIf(e.event_name = 'Mint', 'mint', 'burn') AS event_type,
        r.token0_address                                AS token_address,
        abs(toInt256OrNull(decoded_params['amount0']))  AS amount_raw
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Uniswap V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name IN ('Mint', 'Burn')
      AND e.block_timestamp < today()
      AND decoded_params['amount0'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Uniswap V3'                                    AS protocol,
        r.pool_address                                  AS pool_address,
        lower(decoded_params['owner'])                  AS provider,
        multiIf(e.event_name = 'Mint', 'mint', 'burn') AS event_type,
        r.token1_address                                AS token_address,
        abs(toInt256OrNull(decoded_params['amount1']))  AS amount_raw
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Uniswap V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name IN ('Mint', 'Burn')
      AND e.block_timestamp < today()
      AND decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Swapr V3 — Mint / Burn (identical to Uniswap V3)
-- =============================================
swapr_v3_liquidity AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Swapr V3'                                      AS protocol,
        r.pool_address                                  AS pool_address,
        lower(decoded_params['owner'])                  AS provider,
        multiIf(e.event_name = 'Mint', 'mint', 'burn') AS event_type,
        r.token0_address                                AS token_address,
        abs(toInt256OrNull(decoded_params['amount0']))  AS amount_raw
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Swapr V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name IN ('Mint', 'Burn')
      AND e.block_timestamp < today()
      AND decoded_params['amount0'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}

    UNION ALL

    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Swapr V3'                                      AS protocol,
        r.pool_address                                  AS pool_address,
        lower(decoded_params['owner'])                  AS provider,
        multiIf(e.event_name = 'Mint', 'mint', 'burn') AS event_type,
        r.token1_address                                AS token_address,
        abs(toInt256OrNull(decoded_params['amount1']))  AS amount_raw
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Swapr V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name IN ('Mint', 'Burn')
      AND e.block_timestamp < today()
      AND decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Balancer V2 — PoolBalanceChanged
-- Tokens and deltas are parallel JSON arrays; ARRAY JOIN
-- unpacks them into one row per token per event.
-- Positive delta = token added (mint); negative = removed (burn).
-- =============================================
balancer_v2_liquidity AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V2'                                   AS protocol,
        r.pool_address                                  AS pool_address,
        lower(decoded_params['liquidityProvider'])      AS provider,
        multiIf(
            multiIf(
                startsWith(replaceAll(delta_val, '"', ''), '-'),
                toInt256OrNull(replaceAll(delta_val, '"', '')),
                reinterpretAsInt256(toUInt256OrNull(replaceAll(delta_val, '"', '')))
            ) >= 0,
            'mint', 'burn'
        )                                               AS event_type,
        lower(replaceAll(token_val, '"', ''))           AS token_address,
        abs(multiIf(
            startsWith(replaceAll(delta_val, '"', ''), '-'),
            toInt256OrNull(replaceAll(delta_val, '"', '')),
            reinterpretAsInt256(toUInt256OrNull(replaceAll(delta_val, '"', '')))
        ))                                              AS amount_raw
    FROM {{ ref('contracts_BalancerV2_Vault_events') }} e
    LEFT JOIN balancer_v2_pool_registry r
        ON r.pool_id = lower(decoded_params['poolId'])
    ARRAY JOIN
        JSONExtractArrayRaw(ifNull(decoded_params['tokens'], '[]')) AS token_val,
        JSONExtractArrayRaw(ifNull(decoded_params['deltas'],  '[]')) AS delta_val
    WHERE e.event_name = 'PoolBalanceChanged'
      AND e.block_timestamp < today()
      AND decoded_params['liquidityProvider'] IS NOT NULL
      AND decoded_params['tokens']            IS NOT NULL
      AND decoded_params['deltas']            IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Balancer V3 — LiquidityAdded / LiquidityRemoved
-- amountsRaw is a positional array; token_index resolved via
-- stg_pools__balancer_v3_pool_tokens, then wrapper → underlying
-- via stg_pools__balancer_v3_token_map for price-compatible token_address.
-- =============================================
balancer_v3_liquidity AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V3'                                                                    AS protocol,
        concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))               AS pool_address,
        lower(decoded_params['liquidityProvider'])                                       AS provider,
        multiIf(e.event_name = 'LiquidityAdded', 'mint', 'burn')                        AS event_type,
        -- Prefer underlying address (priced token); fall back to wrapper when not in map
        coalesce(nullIf(wm.underlying_address, ''), nullIf(pt.token_address, ''))         AS token_address,
        abs(toInt256OrNull(replaceAll(amount_val, '"', '')))                             AS amount_raw
    FROM {{ ref('contracts_BalancerV3_Vault_events') }} e
    ARRAY JOIN
        range(length(JSONExtractArrayRaw(ifNull(
            multiIf(
                e.event_name = 'LiquidityAdded',   decoded_params['amountsAddedRaw'],
                e.event_name = 'LiquidityRemoved', decoded_params['amountsRemovedRaw'],
                '[]'
            ), '[]'
        )))) AS token_idx,
        JSONExtractArrayRaw(ifNull(
            multiIf(
                e.event_name = 'LiquidityAdded',   decoded_params['amountsAddedRaw'],
                e.event_name = 'LiquidityRemoved', decoded_params['amountsRemovedRaw'],
                '[]'
            ), '[]'
        )) AS amount_val
    LEFT JOIN {{ ref('stg_pools__balancer_v3_pool_tokens') }} pt
        ON  pt.pool_address = replaceAll(lower(decoded_params['pool']), '0x', '')
        AND pt.token_index  = token_idx
    LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm
        ON wm.wrapper_address = pt.token_address
    WHERE e.event_name IN ('LiquidityAdded', 'LiquidityRemoved')
      AND e.block_timestamp < today()
      AND decoded_params['liquidityProvider'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

all_liquidity AS (
    SELECT * FROM uniswap_v3_liquidity
    UNION ALL
    SELECT * FROM swapr_v3_liquidity
    UNION ALL
    SELECT * FROM balancer_v2_liquidity
    UNION ALL
    SELECT * FROM balancer_v3_liquidity
),

-- =============================================
-- Enrich with token metadata and USD price
-- =============================================
with_meta AS (
    SELECT
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
        l.amount_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))         AS amount
    FROM all_liquidity l
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
        ON  tm.token_address          = l.token_address
        AND toDate(l.block_timestamp) >= toDate(tm.date_start)
),

with_price AS (
    SELECT
        s.*,
        pr.price_usd AS token_price_usd
    FROM with_meta s
    ASOF LEFT JOIN (
        SELECT token, date, price_usd
        FROM {{ ref('stg_pools__token_prices_daily') }}
        ORDER BY token, date
    ) pr
        ON  pr.token                  = s.token_symbol
        AND toDate(s.block_timestamp) >= pr.date
)

SELECT
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
    amount * token_price_usd AS amount_usd
FROM with_price
WHERE amount_raw > 0
ORDER BY block_timestamp, transaction_hash, log_index, token_address
