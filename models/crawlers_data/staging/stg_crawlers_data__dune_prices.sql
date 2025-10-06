{{
  config(
    materialized='view', 
    tags=['staging','crawlers_data']
  )
}}

SELECT
  toDate(block_date)                    AS price_date,
  upper(symbol)                         AS symbol,
  argMax(toFloat64(price), block_date)  AS price   
FROM {{ source('crawlers_data','dune_prices') }}
GROUP BY price_date, symbol
ORDER BY price_date, symbol