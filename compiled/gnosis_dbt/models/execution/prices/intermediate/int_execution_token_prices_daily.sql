

-- Hybrid price hub: native on-chain prices are primary; the Dune feed is kept as a
-- lower-priority fallback for history native cannot reach. Native (Chainlink oracles +
-- DEX-derived + sDAI vault) only exists from ~2021 (Chainlink's Gnosis deployment),
-- whereas Dune's off-chain feed covers 2017+. So for any (date, symbol) native lacks
-- -- mainly pre-2021, plus tokens with no native source (e.g. SAFE) -- Dune fills in.
-- Priority: native (1) > backedfi RWA / aToken wrappers (2) > Dune fallback (3) > $1 pegs (4).

WITH native AS (
    SELECT
        toDate(date)        AS date,
        upper(symbol)       AS symbol,
        toFloat64(price)    AS price
    FROM `dbt`.`int_execution_prices_native_daily`
    WHERE date < today()
),

dune AS (
    -- Historical / gap fallback only (lower priority than native).
    SELECT
        toDate(date)        AS date,
        upper(symbol)       AS symbol,
        toFloat64(price)    AS price
    FROM `dbt`.`stg_crawlers_data__dune_prices`
    WHERE date < today()
),

backedfi AS (
    SELECT
        toDate(date)        AS date,
        upper(bticker)      AS symbol,
        toFloat64(price)    AS price
    FROM `dbt`.`fct_execution_rwa_backedfi_prices_daily`
    WHERE date < today()
),

wxdai_from_xdai AS (
    SELECT
        date,
        'WXDAI' AS symbol,
        price
    FROM native
    WHERE symbol = 'XDAI'
),

wrapper_prices AS (
    -- Supply-token prices (Aave aTokens, Spark spTokens) inherit 1:1 from their reserve.
    SELECT
        p.date,
        upper(m.supply_token_symbol) AS symbol,
        p.price
    FROM `dbt`.`lending_market_mapping` m
    INNER JOIN native p
        ON upper(p.symbol) = upper(m.reserve_symbol)
),

usd_pegs AS (
    SELECT
        date,
        symbol,
        1.0 AS price
    FROM (
        SELECT DISTINCT date FROM native
    )
    ARRAY JOIN ['USDC','USDC.E','USDT'] AS symbol
),

all_prices AS (
    SELECT date, symbol, price, 1 AS priority FROM native
    UNION ALL
    SELECT date, symbol, price, 2 AS priority FROM backedfi
    UNION ALL
    SELECT date, symbol, price, 1 AS priority FROM wxdai_from_xdai
    UNION ALL
    SELECT date, symbol, price, 2 AS priority FROM wrapper_prices
    UNION ALL
    SELECT date, symbol, price, 3 AS priority FROM dune
    UNION ALL
    SELECT date, symbol, 1.0 AS price, 4 AS priority FROM usd_pegs
),

deduplicated AS (
    SELECT
        date,
        symbol,
        price
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
    SELECT
        upper(w.symbol) AS symbol_upper,
        argMax(w.symbol, w.date_start) AS symbol_display
    FROM `dbt`.`tokens_whitelist` w
    GROUP BY symbol_upper
)

SELECT
    d.date,
    coalesce(nullIf(w.symbol_display, ''), d.symbol) AS symbol,
    d.price
FROM deduplicated d
LEFT JOIN whitelist_symbols w
  ON upper(d.symbol) = w.symbol_upper
ORDER BY d.date, symbol