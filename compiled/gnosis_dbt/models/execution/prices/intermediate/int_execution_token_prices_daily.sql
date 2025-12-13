

WITH dune AS (
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
        'WXDAI',
        price
    FROM dune
    WHERE symbol = 'XDAI'
),

agnosdai_from_sdai AS (
    SELECT
        date,
        'AGNOSDAI',
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
)

SELECT date, symbol, price FROM dune
UNION ALL
SELECT date, symbol, price FROM backedfi
UNION ALL
SELECT * FROM wxdai_from_xdai
UNION ALL
SELECT * FROM agnosdai_from_sdai
UNION ALL
SELECT date, symbol, price FROM usd_pegs
ORDER BY date, symbol