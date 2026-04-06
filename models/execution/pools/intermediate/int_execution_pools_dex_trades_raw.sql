{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET join_use_nulls = 0",
            "SET allow_experimental_json_type = 1"
        ],
        tags=['production', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{#-
  Raw DEX swap events across all protocols on Gnosis Chain.
  One row per swap with token metadata (symbol, decimals, human amounts).
  No USD price enrichment — that lives in int_execution_pools_dex_trades.

  Reads directly from contract event decoders (not staging) to access
  decoded swap parameters — recipient, amount0/amount1, tokenIn/tokenOut —
  which the staging layer discards in favour of its long delta format.

  Protocols:
    - Uniswap V3  : recipient from Swap event; token addresses from pool registry
    - Swapr V3    : same pattern (Algebra fork)
    - Balancer V2 : tokenIn/tokenOut direct in event; no taker field available
    - Balancer V3 : same; wrapper → underlying resolution via token map

  Taker notes:
    - UniswapV3/Swapr V3: recipient = address receiving bought tokens (correct
      even when routed through a contract, unlike tx_from which would give the router)
    - Balancer V2/V3: no caller/recipient in Swap event → NULL

  Batching:
    When called with start_month/end_month vars (e.g. via the full-refresh script),
    each protocol CTE filters to that month window. Otherwise falls back to the
    incremental filter based on max(block_timestamp) in the existing table.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

-- =============================================
-- Balancer V2: pool_id → pool_address lookup.
-- Not incrementally filtered: we need ALL historical
-- PoolRegistered events to resolve any swap's pool_id.
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
-- Uniswap V3 Swaps
-- amount0 < 0 → token0 flows OUT of pool (bought by taker)
-- amount0 > 0 → token0 flows INTO pool (sold by taker)
-- =============================================
uniswap_v3_swaps AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Uniswap V3'                                                             AS protocol,
        r.pool_address                                                           AS pool_address,
        CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN r.token0_address ELSE r.token1_address
        END                                                                      AS token_bought_address,
        abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN toInt256OrNull(decoded_params['amount0'])
            ELSE toInt256OrNull(decoded_params['amount1'])
        END)                                                                     AS amount_bought_raw,
        CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN r.token1_address ELSE r.token0_address
        END                                                                      AS token_sold_address,
        abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN toInt256OrNull(decoded_params['amount1'])
            ELSE toInt256OrNull(decoded_params['amount0'])
        END)                                                                     AS amount_sold_raw,
        lower(decoded_params['recipient'])                                       AS taker
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Uniswap V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND decoded_params['amount0'] IS NOT NULL
      AND decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Swapr V3 (Algebra fork) — identical sign logic to Uniswap V3
-- =============================================
swapr_v3_swaps AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Swapr V3'                                                               AS protocol,
        r.pool_address                                                           AS pool_address,
        CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN r.token0_address ELSE r.token1_address
        END                                                                      AS token_bought_address,
        abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN toInt256OrNull(decoded_params['amount0'])
            ELSE toInt256OrNull(decoded_params['amount1'])
        END)                                                                     AS amount_bought_raw,
        CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN r.token1_address ELSE r.token0_address
        END                                                                      AS token_sold_address,
        abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
            THEN toInt256OrNull(decoded_params['amount1'])
            ELSE toInt256OrNull(decoded_params['amount0'])
        END)                                                                     AS amount_sold_raw,
        lower(decoded_params['recipient'])                                       AS taker
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    INNER JOIN (
        SELECT pool_address, pool_address_no0x, token0_address, token1_address
        FROM {{ ref('stg_pools__v3_pool_registry') }}
        WHERE protocol = 'Swapr V3'
    ) r ON r.pool_address_no0x = e.contract_address
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND decoded_params['amount0'] IS NOT NULL
      AND decoded_params['amount1'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Balancer V2 Swaps
-- tokenIn/tokenOut explicit; no taker in event
-- =============================================
balancer_v2_swaps AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V2'                                                            AS protocol,
        r.pool_address                                                           AS pool_address,
        lower(decoded_params['tokenOut'])                                        AS token_bought_address,
        toUInt256OrNull(decoded_params['amountOut'])                             AS amount_bought_raw,
        lower(decoded_params['tokenIn'])                                         AS token_sold_address,
        toUInt256OrNull(decoded_params['amountIn'])                              AS amount_sold_raw,
        CAST(NULL AS Nullable(String))                                           AS taker
    FROM {{ ref('contracts_BalancerV2_Vault_events') }} e
    LEFT JOIN balancer_v2_pool_registry r
        ON r.pool_id = lower(decoded_params['poolId'])
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND decoded_params['tokenIn']   IS NOT NULL
      AND decoded_params['tokenOut']  IS NOT NULL
      AND decoded_params['amountIn']  IS NOT NULL
      AND decoded_params['amountOut'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

-- =============================================
-- Balancer V3 Swaps
-- tokenIn/tokenOut explicit; swapFeeAmount available but not used here
-- (fee analytics stay in int_execution_pools_fees_daily)
-- =============================================
balancer_v3_swaps AS (
    SELECT
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        'Balancer V3'                                                                    AS protocol,
        concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))               AS pool_address,
        coalesce(nullIf(wm_out.underlying_address, ''), lower(decoded_params['tokenOut'])) AS token_bought_address,
        toUInt256OrNull(decoded_params['amountOut'])                                     AS amount_bought_raw,
        coalesce(nullIf(wm_in.underlying_address, ''), lower(decoded_params['tokenIn'])) AS token_sold_address,
        toUInt256OrNull(decoded_params['amountIn'])                                      AS amount_sold_raw,
        CAST(NULL AS Nullable(String))                                                   AS taker
    FROM {{ ref('contracts_BalancerV3_Vault_events') }} e
    LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm_out
        ON wm_out.wrapper_address = lower(decoded_params['tokenOut'])
    LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm_in
        ON wm_in.wrapper_address = lower(decoded_params['tokenIn'])
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND decoded_params['tokenIn']   IS NOT NULL
      AND decoded_params['tokenOut']  IS NOT NULL
      AND decoded_params['amountIn']  IS NOT NULL
      AND decoded_params['amountOut'] IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

all_swaps AS (
    SELECT * FROM uniswap_v3_swaps
    UNION ALL
    SELECT * FROM swapr_v3_swaps
    UNION ALL
    SELECT * FROM balancer_v2_swaps
    UNION ALL
    SELECT * FROM balancer_v3_swaps
)

-- =============================================
-- Enrich with token metadata (symbol + decimals).
-- date_start guards against redeployed tokens with
-- new addresses sharing the same symbol.
-- =============================================
SELECT
    s.block_timestamp,
    s.transaction_hash,
    s.log_index,
    s.protocol,
    s.pool_address,
    s.token_bought_address,
    tb.token                                                                         AS token_bought_symbol,
    s.amount_bought_raw,
    s.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))           AS amount_bought,
    s.token_sold_address,
    ts.token                                                                         AS token_sold_symbol,
    s.amount_sold_raw,
    s.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))             AS amount_sold,
    s.taker
FROM all_swaps s
LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tb
    ON  tb.token_address = s.token_bought_address
    AND toDate(s.block_timestamp) >= toDate(tb.date_start)
LEFT JOIN {{ ref('stg_pools__tokens_meta') }} ts
    ON  ts.token_address = s.token_sold_address
    AND toDate(s.block_timestamp) >= toDate(ts.date_start)
WHERE s.amount_bought_raw > 0
  AND s.amount_sold_raw   > 0
