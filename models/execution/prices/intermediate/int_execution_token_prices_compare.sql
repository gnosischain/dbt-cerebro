{{
  config(
    materialized='view',
    tags=['production', 'execution', 'prices', 'daily', 'external']
  )
}}

-- Side-by-side price compare for validation of the external standbein.
-- Does NOT alter int_execution_token_prices_daily. Reads native + Dune +
-- external sources independently so you can spot divergences (e.g. frozen
-- SAFE DEX forward-fill vs live DefiLlama/CoinGecko).
--
-- Note: grain column is aliased `price_date` inside CTEs — bare `date` as a
-- column name trips ClickHouse UNKNOWN_IDENTIFIER in outer SELECT/ORDER BY.

WITH whitelist_symbols AS (
    SELECT
        upper(w.symbol) AS symbol_upper,
        argMax(w.symbol, w.date_start) AS symbol_display
    FROM {{ ref('tokens_whitelist') }} w
    GROUP BY symbol_upper
),

-- Prices must be Nullable(Float64). Non-nullable Float64 makes ClickHouse
-- LEFT JOIN fill missing matches with 0 (false "price"), which broke
-- source_count / abs_pct_diff for tokens without a CoinGecko series (xDAI/WxDAI).
native AS (
    SELECT
        toDate(n.date) AS price_date,
        upper(n.symbol) AS symbol_upper,
        CAST(n.price AS Nullable(Float64)) AS price
    FROM {{ ref('int_execution_prices_native_daily') }} AS n
    WHERE toDate(n.date) < today()
),

hub AS (
    SELECT
        toDate(h.date) AS price_date,
        upper(h.symbol) AS symbol_upper,
        CAST(h.price AS Nullable(Float64)) AS price
    FROM {{ ref('int_execution_token_prices_daily') }} AS h
    WHERE toDate(h.date) < today()
),

dune AS (
    SELECT
        toDate(d.date) AS price_date,
        upper(d.symbol) AS symbol_upper,
        CAST(d.price AS Nullable(Float64)) AS price
    FROM {{ ref('stg_crawlers_data__dune_prices') }} AS d
    WHERE toDate(d.date) < today()
),

defillama AS (
    SELECT
        toDate(l.date) AS price_date,
        upper(l.symbol) AS symbol_upper,
        CAST(l.price AS Nullable(Float64)) AS price
    FROM {{ ref('stg_crawlers_data__defillama_prices') }} AS l
    WHERE toDate(l.date) < today()
),

coingecko AS (
    SELECT
        toDate(c.date) AS price_date,
        upper(c.symbol) AS symbol_upper,
        CAST(c.price AS Nullable(Float64)) AS price
    FROM {{ ref('stg_crawlers_data__coingecko_prices') }} AS c
    WHERE toDate(c.date) < today()
),

keys AS (
    SELECT price_date, symbol_upper FROM native
    UNION DISTINCT
    SELECT price_date, symbol_upper FROM hub
    UNION DISTINCT
    SELECT price_date, symbol_upper FROM dune
    UNION DISTINCT
    SELECT price_date, symbol_upper FROM defillama
    UNION DISTINCT
    SELECT price_date, symbol_upper FROM coingecko
),

joined AS (
    SELECT
        k.price_date AS price_date,
        coalesce(nullIf(w.symbol_display, ''), k.symbol_upper) AS symbol,
        n.price AS price_native,
        h.price AS price_hub,
        d.price AS price_dune,
        l.price AS price_defillama,
        c.price AS price_coingecko
    FROM keys AS k
    LEFT JOIN whitelist_symbols AS w ON k.symbol_upper = w.symbol_upper
    LEFT JOIN native AS n
        ON k.price_date = n.price_date AND k.symbol_upper = n.symbol_upper
    LEFT JOIN hub AS h
        ON k.price_date = h.price_date AND k.symbol_upper = h.symbol_upper
    LEFT JOIN dune AS d
        ON k.price_date = d.price_date AND k.symbol_upper = d.symbol_upper
    LEFT JOIN defillama AS l
        ON k.price_date = l.price_date AND k.symbol_upper = l.symbol_upper
    LEFT JOIN coingecko AS c
        ON k.price_date = c.price_date AND k.symbol_upper = c.symbol_upper
)

SELECT
    price_date AS date,
    symbol,
    price_native,
    price_hub,
    price_dune,
    price_defillama,
    price_coingecko,
    if(
        price_native > 0 AND price_defillama IS NOT NULL,
        abs(price_native - price_defillama) / price_native,
        NULL
    ) AS abs_pct_diff_native_defillama,
    if(
        price_native > 0 AND price_coingecko IS NOT NULL,
        abs(price_native - price_coingecko) / price_native,
        NULL
    ) AS abs_pct_diff_native_coingecko,
    if(
        price_dune > 0 AND price_defillama IS NOT NULL,
        abs(price_dune - price_defillama) / price_dune,
        NULL
    ) AS abs_pct_diff_dune_defillama,
    if(
        price_dune > 0 AND price_coingecko IS NOT NULL,
        abs(price_dune - price_coingecko) / price_dune,
        NULL
    ) AS abs_pct_diff_dune_coingecko,
    if(
        price_defillama > 0 AND price_coingecko IS NOT NULL,
        abs(price_defillama - price_coingecko) / price_defillama,
        NULL
    ) AS abs_pct_diff_defillama_coingecko,
    if(
        price_hub > 0 AND price_defillama IS NOT NULL,
        abs(price_hub - price_defillama) / price_hub,
        NULL
    ) AS abs_pct_diff_hub_defillama,
    (
        if(price_native IS NOT NULL, 1, 0)
        + if(price_dune IS NOT NULL, 1, 0)
        + if(price_defillama IS NOT NULL, 1, 0)
        + if(price_coingecko IS NOT NULL, 1, 0)
    ) AS source_count
FROM joined
ORDER BY price_date, symbol
