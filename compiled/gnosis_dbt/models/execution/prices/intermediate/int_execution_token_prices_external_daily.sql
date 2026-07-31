

-- Long-form external price standbein (DefiLlama + CoinGecko). Phase 1: not
-- merged into int_execution_token_prices_daily. Symbol display casing follows
-- tokens_whitelist when present.

WITH whitelist_symbols AS (
    SELECT
        upper(w.symbol) AS symbol_upper,
        argMax(w.symbol, w.date_start) AS symbol_display
    FROM `dbt`.`tokens_whitelist` w
    GROUP BY symbol_upper
),

defillama AS (
    SELECT
        toDate(date) AS date,
        upper(symbol) AS symbol_upper,
        'defillama' AS source,
        toFloat64(price) AS price,
        confidence
    FROM `dbt`.`stg_crawlers_data__defillama_prices`
    WHERE date < today()
),

coingecko AS (
    SELECT
        toDate(date) AS date,
        upper(symbol) AS symbol_upper,
        'coingecko' AS source,
        toFloat64(price) AS price,
        CAST(NULL AS Nullable(Float64)) AS confidence
    FROM `dbt`.`stg_crawlers_data__coingecko_prices`
    WHERE date < today()
),

unioned AS (
    SELECT * FROM defillama
    UNION ALL
    SELECT * FROM coingecko
)

SELECT
    u.date,
    coalesce(nullIf(w.symbol_display, ''), u.symbol_upper) AS symbol,
    u.source,
    u.price,
    u.confidence
FROM unioned u
LEFT JOIN whitelist_symbols w
    ON u.symbol_upper = w.symbol_upper
ORDER BY u.date, symbol, u.source