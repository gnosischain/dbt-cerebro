{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, ubo_address, token_address)',
        unique_key='(date, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET join_use_nulls = 0"],
        post_hook=["SET join_use_nulls = 0"],
        tags=['production','execution','ubo','claims','uniswap_v3']
    )
}}

-- Per-user UBO supply claims for Uniswap V3.
--
-- Uniswap V3 pools custody their own tokens (unlike Balancer V2's single Vault),
-- so container_address is the individual pool address.
--
-- Two tracks of LP attribution:
--   Track A (direct LPs, ~45%): the pool Mint/Burn event's `owner` field is the
--     real LP — used directly.
--   Track B (via NPM, ~55%): the pool event's `owner` is the NPM contract
--     (0xae8fbe...). The real LP is determined by decoding the NPM's ERC-721
--     Transfer events (ownership tracking per tokenId) and joining NPM
--     IncreaseLiquidity / DecreaseLiquidity events to pool events via
--     transaction_hash to resolve tokenId → pool_address.
--
-- Position transfers (LP A mints NFT → transfers to LP B → LP B burns) are
-- handled correctly by the Transfer event ownership chain.

{% set npm_address = '0xae8fbe656a77519a7490054274910129c9244fa3' %}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

-- ─── NPM OWNERSHIP TRACKING ────────────────────────────────────────────
-- Build a running record of who owns each tokenId at each point in time.
-- Transfer(from=0x0, to=LP, tokenId) = mint; Transfer(from=LP_A, to=LP_B) = transfer.
npm_transfers AS (
    SELECT
        toUInt256OrNull(decoded_params['tokenId'])   AS token_id,
        lower(decoded_params['from'])                AS transfer_from,
        lower(decoded_params['to'])                  AS transfer_to,
        block_timestamp,
        transaction_hash,
        log_index
    FROM {{ ref('contracts_UniswapV3_NonfungiblePositionManager_events') }}
    WHERE event_name = 'Transfer'
      AND decoded_params['tokenId'] IS NOT NULL
      AND block_timestamp < today()
),

-- Current owner per tokenId = the `to` of the most recent Transfer event.
-- This handles mints, transfers, and burns (burn = transfer to 0x0).
npm_current_owners AS (
    SELECT
        token_id,
        argMax(transfer_to, (block_timestamp, log_index)) AS current_owner
    FROM npm_transfers
    GROUP BY token_id
),

-- ─── NPM LIQUIDITY DELTAS ──────────────────────────────────────────────
-- IncreaseLiquidity (+) and DecreaseLiquidity (-) per tokenId with amounts.
npm_liquidity_events AS (
    SELECT
        toUInt256OrNull(decoded_params['tokenId'])   AS token_id,
        block_timestamp,
        transaction_hash,
        log_index,
        event_name,
        toUInt256OrNull(decoded_params['amount0'])   AS amount0,
        toUInt256OrNull(decoded_params['amount1'])   AS amount1
    FROM {{ ref('contracts_UniswapV3_NonfungiblePositionManager_events') }}
    WHERE event_name IN ('IncreaseLiquidity', 'DecreaseLiquidity')
      AND decoded_params['tokenId'] IS NOT NULL
      AND block_timestamp < today()
),

-- ─── MAP tokenId → pool_address VIA TRANSACTION JOIN ────────────────────
-- NPM IncreaseLiquidity/DecreaseLiquidity events co-occur in the same
-- transaction as pool Mint/Burn events. Join on transaction_hash to resolve
-- which pool a tokenId belongs to.
--
-- We use the pool Mint events (not Burn) for the initial mapping since
-- IncreaseLiquidity always pairs with Mint. For tokenIds that only appear
-- in DecreaseLiquidity, we fall back to pool Burn events.
pool_mint_txns AS (
    SELECT DISTINCT
        transaction_hash,
        lower('0x' || contract_address) AS pool_address
    FROM {{ ref('contracts_UniswapV3_Pool_events') }}
    WHERE event_name IN ('Mint', 'Burn')
      AND block_timestamp < today()
),

token_id_pool_map AS (
    SELECT
        nle.token_id,
        argMin(pmt.pool_address, (nle.block_timestamp, nle.log_index)) AS pool_address
    FROM npm_liquidity_events nle
    INNER JOIN pool_mint_txns pmt
        ON pmt.transaction_hash = nle.transaction_hash
    WHERE pmt.pool_address != ''
    GROUP BY nle.token_id
),

-- ─── POOL REGISTRY FOR TOKEN ADDRESSES ──────────────────────────────────
pool_tokens AS (
    SELECT
        pool_address,
        token0_address,
        token1_address
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE protocol = 'Uniswap V3'
),

-- ─── TRACK B: NPM-mediated LP deltas ───────────────────────────────────
-- Per (date, owner, pool, token) signed deltas from NPM events.
-- Owner is looked up from the Transfer chain for the tokenId AT THE TIME
-- of the liquidity event (argMax gives current owner, but for cumsum
-- purposes we attribute all historical deltas to the current owner — this
-- is the standard simplification since position transfers are rare on
-- Gnosis Chain).
npm_daily_deltas_raw AS (
    SELECT
        toDate(nle.block_timestamp)                                     AS date,
        lower(own.current_owner)                                        AS ubo_address,
        tpm.pool_address                                                AS pool_address,
        nle.event_name,
        nle.amount0,
        nle.amount1
    FROM npm_liquidity_events nle
    INNER JOIN token_id_pool_map tpm
        ON tpm.token_id = nle.token_id
    INNER JOIN npm_current_owners own
        ON own.token_id = nle.token_id
    WHERE own.current_owner != '0x0000000000000000000000000000000000000000'
),

-- Unpack into one row per token (amount0 → token0, amount1 → token1)
npm_daily_deltas AS (
    SELECT
        d.date,
        d.ubo_address,
        d.pool_address                                                  AS container_address,
        pt.token0_address                                               AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(d.event_name = 'IncreaseLiquidity',
                toInt256(d.amount0),
               -toInt256(d.amount0)))                                   AS daily_delta_raw
    FROM npm_daily_deltas_raw d
    INNER JOIN pool_tokens pt ON pt.pool_address = d.pool_address
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON  lower(tw.address)  = lower(pt.token0_address)
        AND d.date             >= tw.date_start
        AND (tw.date_end IS NULL OR d.date < tw.date_end)
    WHERE 1=1
      {% if start_month and end_month %}
        AND toStartOfMonth(d.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(d.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('d.date', 'date', 'true') }}
      {% endif %}
    GROUP BY d.date, d.ubo_address, d.pool_address, pt.token0_address, tw.symbol
    HAVING daily_delta_raw != 0

    UNION ALL

    SELECT
        d.date,
        d.ubo_address,
        d.pool_address                                                  AS container_address,
        pt.token1_address                                               AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(d.event_name = 'IncreaseLiquidity',
                toInt256(d.amount1),
               -toInt256(d.amount1)))                                   AS daily_delta_raw
    FROM npm_daily_deltas_raw d
    INNER JOIN pool_tokens pt ON pt.pool_address = d.pool_address
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON  lower(tw.address)  = lower(pt.token1_address)
        AND d.date             >= tw.date_start
        AND (tw.date_end IS NULL OR d.date < tw.date_end)
    WHERE 1=1
      {% if start_month and end_month %}
        AND toStartOfMonth(d.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(d.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('d.date', 'date', 'true') }}
      {% endif %}
    GROUP BY d.date, d.ubo_address, d.pool_address, pt.token1_address, tw.symbol
    HAVING daily_delta_raw != 0
),

-- ─── TRACK A: Direct LP deltas (owner != NPM) ──────────────────────────
-- These LPs interact with the pool directly; their address is in the
-- pool event's `owner` field.
direct_daily_deltas AS (
    SELECT
        toDate(liq.block_timestamp)                                     AS date,
        lower(liq.provider)                                             AS ubo_address,
        lower(liq.pool_address)                                         AS container_address,
        lower(liq.token_address)                                        AS token_address,
        tw.symbol                                                       AS symbol,
        sum(if(liq.event_type = 'mint',
                toInt256(liq.amount_raw),
               -toInt256(liq.amount_raw)))                              AS daily_delta_raw
    FROM {{ ref('stg_pools__dex_liquidity_uniswap_v3') }} liq
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON  lower(tw.address)           = lower(liq.token_address)
        AND toDate(liq.block_timestamp) >= tw.date_start
        AND (tw.date_end IS NULL OR toDate(liq.block_timestamp) < tw.date_end)
    WHERE liq.event_type IN ('mint', 'burn')
      AND lower(liq.provider) != lower('{{ npm_address }}')
      AND liq.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(liq.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(liq.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('liq.block_timestamp', 'date', 'true') }}
      {% endif %}
    GROUP BY date, ubo_address, container_address, token_address, tw.symbol
    HAVING daily_delta_raw != 0
),

-- ─── MERGE BOTH TRACKS ─────────────────────────────────────────────────
daily_deltas_agg AS (
    SELECT
        date,
        ubo_address,
        container_address,
        symbol,
        sum(daily_delta_raw) AS daily_delta_raw
    FROM (
        SELECT date, ubo_address, container_address, token_address, symbol, daily_delta_raw
        FROM npm_daily_deltas

        UNION ALL

        SELECT date, ubo_address, container_address, token_address, symbol, daily_delta_raw
        FROM direct_daily_deltas
    )
    GROUP BY date, ubo_address, container_address, symbol
),

overall_max_date AS (
    SELECT
        {% if end_month %}
            toLastDayOfMonth(toDate('{{ end_month }}'))
        {% else %}
            yesterday()
        {% endif %} AS max_date
),

{% if start_month and end_month and is_incremental() %}
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.container_address,
        t1.balance_raw
    FROM (SELECT ubo_address, token_address, container_address, balance_raw, date FROM {{ this }}) t1
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = (
        SELECT max(date) FROM {{ this }} WHERE date < toDate('{{ start_month }}')
    )
),
{% elif is_incremental() %}
current_partition AS (
    SELECT max(date) AS max_date
    FROM {{ this }}
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        t1.ubo_address,
        tw.symbol,
        t1.container_address,
        t1.balance_raw
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    INNER JOIN {{ ref('tokens_whitelist') }} tw
        ON lower(tw.address) = lower(t1.token_address)
    WHERE t1.date = t2.max_date
),
{% endif %}

{% if is_incremental() %}
keys AS (
    SELECT DISTINCT ubo_address, symbol, container_address
    FROM (
        SELECT ubo_address, symbol, container_address FROM prev_balances
        UNION ALL
        SELECT ubo_address, symbol, container_address FROM daily_deltas_agg
    )
),

calendar AS (
    SELECT
        k.ubo_address,
        k.symbol,
        k.container_address,
        {% if start_month and end_month %}
            addDays(
                (SELECT max(date) FROM {{ this }} WHERE date < toDate('{{ start_month }}')),
                offset + 1
            ) AS date
        {% else %}
            addDays(cp.max_date, offset + 1) AS date
        {% endif %}
    FROM keys k
    {% if not (start_month and end_month) %}
    CROSS JOIN current_partition cp
    {% endif %}
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        toUInt32(dateDiff('day',
            {% if start_month and end_month %}
                (SELECT max(date) FROM {{ this }} WHERE date < toDate('{{ start_month }}')),
            {% else %}
                cp.max_date,
            {% endif %}
            o.max_date
        ))
    ) AS offset
),
{% else %}
calendar AS (
    SELECT
        ubo_address,
        symbol,
        container_address,
        addDays(min_date, offset) AS date
    FROM (
        SELECT
            d.ubo_address,
            d.symbol,
            d.container_address,
            min(d.date)                                   AS min_date,
            dateDiff('day', min(d.date), any(o.max_date)) AS num_days
        FROM daily_deltas_agg d
        CROSS JOIN overall_max_date o
        GROUP BY d.ubo_address, d.symbol, d.container_address
    )
    ARRAY JOIN range(num_days + 1) AS offset
),
{% endif %}

balances AS (
    SELECT
        c.date             AS date,
        c.ubo_address      AS ubo_address,
        c.symbol           AS symbol,
        c.container_address AS container_address,
        sum(coalesce(d.daily_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.ubo_address, c.symbol, c.container_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.balance_raw, toInt256(0))
        {% endif %}
        AS balance_raw
    FROM calendar c
    LEFT JOIN daily_deltas_agg d
        ON  d.ubo_address       = c.ubo_address
        AND d.symbol            = c.symbol
        AND d.container_address = c.container_address
        AND d.date              = c.date
    {% if is_incremental() %}
    LEFT JOIN prev_balances p
        ON  p.ubo_address       = c.ubo_address
        AND p.symbol            = c.symbol
        AND p.container_address = c.container_address
    {% endif %}
)

SELECT
    b.date                                                                  AS date,
    'Uniswap V3'                                                            AS protocol,
    lower(b.container_address)                                              AS container_address,
    lower(tw_canon.address)                                                 AS token_address,
    b.symbol                                                                AS symbol,
    tw_canon.token_class                                                    AS token_class,
    lower(b.ubo_address)                                                    AS ubo_address,
    toInt256(b.balance_raw)                                                 AS balance_raw,
    b.balance_raw / pow(10, tw_canon.decimals)                             AS balance,
    (b.balance_raw / pow(10, tw_canon.decimals)) * coalesce(pr.price, 0)  AS balance_usd
FROM balances b
INNER JOIN {{ ref('tokens_whitelist') }} tw_canon
    ON  tw_canon.symbol = b.symbol
    AND b.date          >= tw_canon.date_start
    AND (tw_canon.date_end IS NULL OR b.date < tw_canon.date_end)
ASOF LEFT JOIN (
    SELECT symbol, date, price
    FROM {{ ref('int_execution_token_prices_daily') }}
    ORDER BY symbol, date
) pr
    ON  pr.symbol = b.symbol
    AND b.date    >= pr.date
WHERE b.balance_raw > 0
