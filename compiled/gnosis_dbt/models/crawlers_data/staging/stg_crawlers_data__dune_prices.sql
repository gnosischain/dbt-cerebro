

SELECT
  toDate(block_date)                    AS date,
  upper(symbol)                         AS symbol,
  anyLast(toFloat64(price))             AS price   
FROM `crawlers_data`.`dune_prices`
GROUP BY date, symbol
ORDER BY date, symbol