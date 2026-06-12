{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date, symbol)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_memory_usage = 8000000000",
            "SET max_bytes_before_external_group_by = 4000000000"
        ],
        post_hook=[
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_group_by = 0"
        ],
        tags=['production','execution','prices','dex','intermediate','granularity:daily']
    )
}}

-- Daily DEX-derived USD prices for whitelist tokens that have NO Gnosis Chainlink
-- feed: GBPe, BRLA, BRZ, COW, SAFE, sGNO. For each trade where the target token is
-- swapped against an oracle-priced "anchor" token, the target's implied USD price is
-- (anchor_amount * anchor_usd) / target_amount. We take the daily median over such
-- trades, requiring a >= $1000 notional and >= 5 trades/day (Dune's guardrails).
--
-- Sources are strictly UNPRICED to avoid a dependency cycle with the price hub:
--   * int_execution_pools_dex_trades_raw (Uniswap V3 / Swapr V3 / Balancer V2/V3)
--   * stg_cow__trades  (CoW; decimals/symbols joined from tokens_meta here, NOT the
--     priced int_execution_cow_trades, which refs the hub and would cycle).
-- The USD anchor is the native oracle feed int_execution_prices_oracle_daily.
-- BRZ<-BRLA fallback is applied downstream in the assembly model, not here.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH unpriced_trades AS (

    -- Pools: already decimal-adjusted with resolved symbols.
    SELECT
        toDate(block_timestamp)     AS date,
        token_bought_symbol         AS sym_bought,
        amount_bought               AS amt_bought,
        token_sold_symbol           AS sym_sold,
        amount_sold                 AS amt_sold
    FROM {{ ref('int_execution_pools_dex_trades_raw') }}
    WHERE amount_bought > 0
      AND amount_sold   > 0
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
      {% endif %}

    UNION ALL

    -- CoW: unpriced; resolve symbol + decimals from tokens_meta (no hub dependency).
    SELECT
        toDate(t.block_timestamp)                                                       AS date,
        tb.token                                                                        AS sym_bought,
        t.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))           AS amt_bought,
        ts.token                                                                        AS sym_sold,
        t.amount_sold_raw   / POWER(10, if(ts.decimals > 0, ts.decimals, 18))           AS amt_sold
    FROM {{ ref('stg_cow__trades') }} t
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tb
        ON  tb.token_address = t.token_bought_address
        AND toDate(t.block_timestamp) >= toDate(tb.date_start)
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} ts
        ON  ts.token_address = t.token_sold_address
        AND toDate(t.block_timestamp) >= toDate(ts.date_start)
    WHERE t.amount_bought_raw > 0
      AND t.amount_sold_raw   > 0
      AND t.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(t.block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(t.block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.block_timestamp', 'date', 'true') }}
      {% endif %}
),

anchor AS (
    SELECT upper(symbol) AS sym_upper, date, price
    FROM {{ ref('int_execution_prices_oracle_daily') }}
    WHERE price > 0
),

legs AS (

    -- target token was SOLD, an anchor token was BOUGHT
    SELECT
        t.date,
        t.sym_sold                                          AS symbol,
        t.amt_bought * a.price / nullIf(t.amt_sold, 0)      AS implied_usd,
        t.amt_bought * a.price                              AS trade_usd
    FROM unpriced_trades t
    INNER JOIN anchor a
        ON a.sym_upper = upper(t.sym_bought)
       AND a.date      = t.date
    WHERE upper(t.sym_sold) IN ('GBPE','BRLA','BRZ','COW','SAFE','SGNO')

    UNION ALL

    -- target token was BOUGHT, an anchor token was SOLD
    SELECT
        t.date,
        t.sym_bought                                        AS symbol,
        t.amt_sold * a.price / nullIf(t.amt_bought, 0)      AS implied_usd,
        t.amt_sold * a.price                                AS trade_usd
    FROM unpriced_trades t
    INNER JOIN anchor a
        ON a.sym_upper = upper(t.sym_sold)
       AND a.date      = t.date
    WHERE upper(t.sym_bought) IN ('GBPE','BRLA','BRZ','COW','SAFE','SGNO')
)

SELECT
    symbol,
    date,
    quantileExact(0.5)(implied_usd)     AS price,
    count()                             AS n_trades
FROM legs
WHERE trade_usd  >= 1000
  AND implied_usd > 0
GROUP BY symbol, date
HAVING count() >= 5
