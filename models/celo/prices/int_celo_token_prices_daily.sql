{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, symbol)',
    partition_by='toStartOfYear(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','celo','prices','daily']
  )
}}

-- Celo price hub (native-first, same design as the Gnosis
-- int_execution_token_prices_daily but with a smaller source set):
--   1. Chainlink AnswerUpdated decoded from celo_execution.logs (CELO, USDT,
--      USDC, USDm, plus EUR/GBP FX for future display currencies)
--   2. Dune off-chain feed fallback for symbols/dates native cannot reach
--      (pre-backfill history, and XAUt0 which has no Chainlink feed on Celo)
--   3. $1 peg last-resort for the card stablecoins
--
-- Daily price = last answer of the day (Chainlink answers are already
-- outlier-filtered at the oracle level; last-of-day matches how the Gnosis
-- oracle model summarises a day). Forward-fill is deliberately NOT applied
-- here yet — the celo_execution backfill is still in progress, and
-- forward-filling across the backfill frontier would fabricate long stale
-- runs. Revisit once the indexer follows head.

WITH chainlink_daily AS (
    SELECT
        toDate(e.block_timestamp)                                       AS date,
        f.base_symbol                                                   AS symbol,
        argMax(
            toFloat64OrNull(e.decoded_params['current']) / pow(10, f.decimals),
            (e.block_timestamp, e.log_index)
        )                                                               AS price
    FROM {{ ref('contracts_celo_chainlink_feeds_events') }} e
    INNER JOIN {{ ref('celo_chainlink_feeds') }} f
        ON lower(replaceAll(e.contract_address, '0x', ''))
         = lower(replaceAll(f.aggregator_address, '0x', ''))
    WHERE e.event_name = 'AnswerUpdated'
      AND f.quote_symbol = 'USD'
    GROUP BY date, symbol
    HAVING price > 0
),

dune AS (
    SELECT
        toDate(date)     AS date,
        upper(symbol)    AS symbol,
        toFloat64(price) AS price
    FROM {{ ref('stg_crawlers_data__dune_prices') }}
    WHERE date < today()
),

usd_pegs AS (
    SELECT
        date,
        symbol,
        1.0 AS price
    FROM (SELECT DISTINCT date FROM chainlink_daily)
    ARRAY JOIN ['USDT', 'USDC', 'USDM'] AS symbol
),

all_prices AS (
    SELECT date, upper(symbol) AS symbol, price, 1 AS priority FROM chainlink_daily
    UNION ALL
    SELECT date, symbol, price, 2 AS priority FROM dune
    UNION ALL
    SELECT date, symbol, price, 3 AS priority FROM usd_pegs
),

deduplicated AS (
    SELECT date, symbol, price
    FROM (
        SELECT
            date,
            symbol,
            price,
            row_number() OVER (PARTITION BY date, symbol ORDER BY priority) AS rn
        FROM all_prices
    )
    WHERE rn = 1
),

whitelist_symbols AS (
    -- Restore display casing (USDm, XAUt0) from the whitelist seed.
    SELECT
        upper(w.symbol)                AS symbol_upper,
        argMax(w.symbol, w.date_start) AS symbol_display
    FROM {{ ref('celo_tokens_whitelist') }} w
    GROUP BY symbol_upper
)

SELECT
    d.date,
    coalesce(nullIf(w.symbol_display, ''), d.symbol) AS symbol,
    d.price
FROM deduplicated d
LEFT JOIN whitelist_symbols w
  ON d.symbol = w.symbol_upper
ORDER BY d.date, symbol
