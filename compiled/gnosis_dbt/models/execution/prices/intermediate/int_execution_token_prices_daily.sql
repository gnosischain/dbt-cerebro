

-- Hybrid price hub: native on-chain prices are primary; the Dune feed is kept as a
-- lower-priority fallback for history native cannot reach. Native (Chainlink oracles +
-- DEX-derived + sDAI vault) only exists from ~2021 (Chainlink's Gnosis deployment),
-- whereas Dune's off-chain feed covers 2017+. So for any (date, symbol) native lacks
-- -- mainly pre-2021, plus tokens with no native source (e.g. SAFE) -- Dune fills in.
-- Priority: FRESH native (1) > backedfi RWA / aToken wrappers (2) > Dune fallback (3) >
-- $1 pegs (4) > STALE native (5, last resort).
--
-- Staleness demotion: native forward-fills every symbol across a daily calendar, so a
-- DEX-only token that loses liquidity would otherwise serve its last trade price forever
-- at priority 1 (e.g. SAFE froze for >250d at ~4.3x its live value; COW at +14%). We
-- therefore demote a native price BELOW Dune once it has been forward-filled for more
-- than 7 days past its last real
-- observation, so the dense live Dune feed wins. Stale native is kept as priority 5 so a
-- symbol with no Dune coverage still gets a (clearly-flagged-stale) value rather than a gap.



WITH native AS (
    SELECT
        toDate(date)             AS date,
        upper(symbol)            AS symbol,
        toFloat64(price)         AS price,
        toDate(last_obs_date)    AS last_obs_date
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

ocsdai_price AS (
    -- OpenCover OC-sDAI is an ERC-4626 sDAI vault: 1 share is worth `share_price`
    -- sDAI. Value it via the sDAI reserve's native price (look-through) -- exactly
    -- how wrapper_prices inherits an aToken/spToken from its reserve, but with the
    -- vault's share exchange rate instead of 1:1. share_price
    -- (int_yields_ocsdai_share_price_daily, reconstructed from the vault's ERC-4626
    -- events) and native SDAI are both dense from the 2026-03-16 launch. Priority 2.
    SELECT
        sp.date,
        'OC-SDAI' AS symbol,
        n.price * sp.share_price AS price
    FROM `dbt`.`int_yields_ocsdai_share_price_daily` sp
    INNER JOIN native n
        ON n.date = sp.date
       AND n.symbol = 'SDAI'
    WHERE sp.share_price IS NOT NULL
),

all_prices AS (
    -- Fresh native = priority 1; native forward-filled beyond the staleness budget is
    -- demoted to priority 5 (below Dune) so a stale frozen DEX price stops winning.
    SELECT
        date,
        symbol,
        price,
        if(dateDiff('day', last_obs_date, date) > 7, 5, 1) AS priority
    FROM native
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
    UNION ALL
    SELECT date, symbol, price, 2 AS priority FROM ocsdai_price
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