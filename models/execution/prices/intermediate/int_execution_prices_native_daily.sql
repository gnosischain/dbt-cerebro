{{
    config(
        materialized='table',
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
        tags=['production','execution','prices','native','intermediate','granularity:daily']
    )
}}

-- Native daily USD price series for whitelist base tokens, assembled entirely from
-- on-chain data to replace the externally-ingested Dune feed. Combines:
--   * Chainlink oracle prices            (int_execution_prices_oracle_daily)  [authoritative]
--   * sDAI ERC4626 vault rate * xDAI USD (int_yields_savings_xdai_rate_daily)
--   * sGNO ~= GNO
--   * DEX-derived stablecoins            (int_execution_prices_dex_ratios)    [GBPe/BRLA/BRZ/COW/SAFE]
-- then nulls DEX outliers with a 30-day rolling MAD, applies a BRZ<-BRLA fallback,
-- and forward-fills every symbol across a daily calendar so the series is dense.
--
-- Full rebuild each run (the rolling window + forward-fill from each symbol's first
-- observation need full history); cheap because the inputs are small daily tables.
-- Output (date, symbol, price) is a drop-in for the hub's Dune CTE. Base tokens only:
-- the hub still layers aTokens (lending_market_mapping) and RWA (backedfi) on top.
-- See docs/native_token_prices_build_plan.md.

WITH oracle AS (
    SELECT symbol, toDate(date) AS date, price
    FROM {{ ref('int_execution_prices_oracle_daily') }}
    WHERE price > 0
),

xdai AS (
    SELECT date, price AS xdai_usd FROM oracle WHERE symbol = 'xDAI'
),

-- sDAI: vault share price is denominated in xDAI; multiply by xDAI USD.
sdai AS (
    SELECT 'sDAI' AS symbol, r.date, r.share_price * x.xdai_usd AS price
    FROM (
        SELECT toDate(date) AS date, share_price
        FROM {{ ref('int_yields_savings_xdai_rate_daily') }}
        WHERE share_price > 0
    ) r
    INNER JOIN xdai x ON x.date = r.date
),

-- DEX-derived series + 30-day rolling median / MAD outlier nulling.
-- Now includes sGNO (staked GNO) priced from its own DEX trades against GNO/anchors.
dex_raw AS (
    SELECT symbol, date, price
    FROM {{ ref('int_execution_prices_dex_ratios') }}
    WHERE price > 0
),
dex_med AS (
    SELECT symbol, date, price,
        quantileExact(0.5)(price) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS med30
    FROM dex_raw
),
dex_mad AS (
    SELECT symbol, date, price, med30,
        quantileExact(0.5)(abs(price - med30)) OVER (
            PARTITION BY symbol ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS mad30
    FROM dex_med
),
dex_clean AS (
    SELECT symbol, date, price
    FROM dex_mad
    WHERE mad30 = 0 OR abs(price - med30) <= 3 * mad30
),

-- Priority union: authoritative (0) > DEX own price (1) > BRZ<-BRLA fallback (2).
combined AS (
    SELECT symbol, date, price, 0 AS prio FROM oracle
    UNION ALL SELECT symbol, date, price, 0 AS prio FROM sdai
    UNION ALL SELECT symbol, date, price, 1 AS prio FROM dex_clean
    -- BRZ <- BRLA fallback. Filter on the real symbol in an inner subquery before
    -- relabeling (ClickHouse alias-shadows-column-in-WHERE pitfall).
    UNION ALL SELECT 'BRZ' AS symbol, date, price, 2 AS prio FROM (SELECT date, price FROM dex_clean WHERE symbol = 'BRLA')
    -- sGNO <- GNO fallback for periods with no sGNO DEX price; sGNO's own market
    -- price (from dex_clean, priority 1) wins when available.
    UNION ALL SELECT 'sGNO' AS symbol, date, price, 2 AS prio FROM (SELECT date, price FROM oracle WHERE symbol = 'GNO')
),

deduped AS (
    SELECT symbol, date, price
    FROM (
        SELECT symbol, date, price,
               row_number() OVER (PARTITION BY symbol, date ORDER BY prio) AS rn
        FROM combined
    )
    WHERE rn = 1
),

-- Per-symbol daily calendar from each symbol's first observation, then forward-fill.
bounds AS (
    SELECT symbol, min(date) AS first_date FROM deduped GROUP BY symbol
),
spine AS (
    SELECT b.symbol, s.day AS date
    FROM bounds b
    CROSS JOIN {{ ref('dim_time_spine_daily') }} s
    WHERE s.day >= b.first_date
      AND s.day <  today()
),
filled AS (
    SELECT
        sp.symbol,
        sp.date,
        last_value(d.price) IGNORE NULLS OVER (
            PARTITION BY sp.symbol ORDER BY sp.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS price
    FROM spine sp
    LEFT JOIN deduped d ON d.symbol = sp.symbol AND d.date = sp.date
)

SELECT date, symbol, price
FROM filled
WHERE price IS NOT NULL
