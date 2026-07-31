

-- Deduped CoinGecko external prices. Append-only raw table; anyLast wins for
-- re-runs on the same (date, symbol) grain.

SELECT
    toDate(block_date)                    AS date,
    upper(symbol)                         AS symbol,
    anyLast(coingecko_id)                 AS coingecko_id,
    anyLast(toFloat64(price))             AS price
FROM `crawlers_data`.`coingecko_prices`
GROUP BY date, symbol
ORDER BY date, symbol