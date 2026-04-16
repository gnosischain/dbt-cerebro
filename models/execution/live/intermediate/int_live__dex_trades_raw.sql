{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        ttl='block_timestamp + INTERVAL 48 HOUR',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'live', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{#
    Cached, rolling 48h window of unified DEX swaps across all supported
    protocols (Uniswap V3, Swapr V3, Balancer V2, Balancer V3) on Gnosis Chain.

    MATERIALIZATION
    ---------------
    Incremental ReplacingMergeTree, refreshed by a k8s CronJob running
    `dbt run --select +tag:live` every ~5 min.
    - First run (non-incremental): loads last 48h from source HWM — matches
      source TTL, so we don't exceed what's recoverable.
    - Each subsequent run: delete+insert the last `live_trades_overlap_minutes`
      of data (default 120 = 2h). This self-heals against cryo-live's
      out-of-order / bulk-attach inserts, as long as backfills don't arrive
      more than 2h late. Widen the var if you observe gaps.
    - 48h TTL on block_timestamp prevents unbounded growth; matches source.

    Downstream api views read this cached table directly, so dashboard
    queries never trigger decode-log scans over `execution_live.logs`.

    COLUMNS
    -------
    Normalized amounts (via token decimals), resolved symbols, USD prices
    per side (ASOF join on daily prices), and a conservative `amount_usd`
    that takes the LEAST of both priced sides (rejects inflated long-tail
    prices). NULLs where prices are unknown.
#}

{%- set overlap_minutes = var('live_trades_overlap_minutes', 120) -%}

WITH

all_swaps AS (
    SELECT * FROM {{ ref('stg_live__dex_trades_uniswap_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_live__dex_trades_swapr_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_live__dex_trades_balancer_v2') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_live__dex_trades_balancer_v3') }}
),

normalized AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.transaction_hash,
        s.log_index,
        s.protocol,
        s.pool_address,
        s.token_bought_address,
        tb.token                                                               AS token_bought_symbol,
        s.amount_bought_raw,
        s.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))  AS amount_bought,
        s.token_sold_address,
        ts.token                                                               AS token_sold_symbol,
        s.amount_sold_raw,
        s.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))    AS amount_sold
    FROM all_swaps s
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tb
        ON  tb.token_address = s.token_bought_address
        AND toDate(s.block_timestamp) >= toDate(tb.date_start)
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} ts
        ON  ts.token_address = s.token_sold_address
        AND toDate(s.block_timestamp) >= toDate(ts.date_start)
    WHERE s.amount_bought_raw > 0
      AND s.amount_sold_raw   > 0
      {% if is_incremental() %}
      AND s.block_timestamp >= (
          SELECT addMinutes(max(block_timestamp), -{{ overlap_minutes }})
          FROM {{ this }}
      )
      {% else %}
      AND s.block_timestamp >= (
          SELECT max(block_timestamp) FROM {{ source('execution_live', 'logs') }}
      ) - INTERVAL 48 HOUR
      {% endif %}
),

with_bought_price AS (
    SELECT
        n.*,
        pb.price AS token_bought_price_usd
    FROM normalized n
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM {{ ref('int_execution_token_prices_daily') }}
        WHERE date >= today() - 7
        ORDER BY symbol, date
    ) pb
        ON  pb.symbol                 = n.token_bought_symbol
        AND toDate(n.block_timestamp) >= pb.date
),

with_sold_price AS (
    SELECT
        b.*,
        ps.price AS token_sold_price_usd
    FROM with_bought_price b
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM {{ ref('int_execution_token_prices_daily') }}
        WHERE date >= today() - 7
        ORDER BY symbol, date
    ) ps
        ON  ps.symbol                 = b.token_sold_symbol
        AND toDate(b.block_timestamp) >= ps.date
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    token_bought_address,
    token_bought_symbol,
    amount_bought_raw,
    amount_bought,
    token_bought_price_usd,
    amount_bought * token_bought_price_usd                                   AS amount_bought_usd,
    token_sold_address,
    token_sold_symbol,
    amount_sold_raw,
    amount_sold,
    token_sold_price_usd,
    amount_sold * token_sold_price_usd                                       AS amount_sold_usd,
    {#
        Conservative USD notional:
        - If both sides priced, take the LEAST of the two. This rejects inflated
          long-tail prices: a $10M "trade" of a shitcoin against $200 of USDC
          becomes $200, not $10M.
        - If only one side priced, use that.
        - If neither priced, NULL (downstream decides whether to drop or show).
    #}
    CASE
        WHEN token_bought_price_usd IS NOT NULL AND token_sold_price_usd IS NOT NULL
            THEN least(
                amount_bought * token_bought_price_usd,
                amount_sold   * token_sold_price_usd
            )
        ELSE coalesce(
            amount_bought * token_bought_price_usd,
            amount_sold   * token_sold_price_usd
        )
    END                                                                      AS amount_usd
FROM with_sold_price
