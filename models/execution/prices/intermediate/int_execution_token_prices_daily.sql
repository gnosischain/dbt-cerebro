{{
  config(
    materialized='view',
    tags=['production','execution','prices','daily']
  )
}}

WITH dune AS (
    SELECT
        toDate(date)        AS date,
        upper(symbol)       AS symbol,
        toFloat64(price)    AS price
    FROM {{ ref('stg_crawlers_data__dune_prices') }}
    WHERE date < today()
),

backedfi AS (
    SELECT
        toDate(date)        AS date,
        upper(bticker)      AS symbol,
        toFloat64(price)    AS price
    FROM {{ ref('fct_execution_rwa_backedfi_prices_daily') }}
    WHERE date < today()
),

wxdai_from_xdai AS (
    SELECT
        date,
        'WXDAI' AS symbol,
        price
    FROM dune
    WHERE symbol = 'XDAI'
),

agnosdai_from_sdai AS (
    SELECT
        date,
        'AGNOSDAI' AS symbol,
        price
    FROM dune
    WHERE symbol = 'SDAI'
),

usd_pegs AS (
    SELECT
        date,
        symbol,
        1.0 AS price
    FROM (
        SELECT DISTINCT date FROM dune
    )
    ARRAY JOIN ['USDC','USDC.E','USDT'] AS symbol
),

all_prices AS (
    SELECT date, symbol, price, 1 AS priority FROM dune
    UNION ALL
    SELECT date, symbol, price, 2 AS priority FROM backedfi
    UNION ALL
    SELECT date, symbol, price, 1 AS priority FROM wxdai_from_xdai
    UNION ALL
    SELECT date, symbol, price, 1 AS priority FROM agnosdai_from_sdai
    UNION ALL
    SELECT date, symbol, 1.0 AS price, 3 AS priority FROM usd_pegs
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
    FROM {{ ref('tokens_whitelist') }} w
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