{{
  config(
    materialized='view',
    tags=['staging','crawlers_data']
  )
}}

WITH base AS (
  SELECT
    toDate(block_date) AS price_date,
    upper(symbol)      AS symbol,
    toFloat64(price)   AS price
  FROM {{ source('playground_max','dune_prices') }}
  WHERE upper(symbol) IN ('XDAI','DAI')
),
xdai AS (
  SELECT
    price_date,
    price AS price_xdai
  FROM base
  WHERE symbol = 'XDAI'
  ORDER BY price_date
  LIMIT 1 BY price_date         -- 1 row per day without aggregates
),
dai AS (
  SELECT
    price_date,
    price AS price_dai
  FROM base
  WHERE symbol = 'DAI'
  ORDER BY price_date
  LIMIT 1 BY price_date
)
SELECT
  coalesce(x.price_date, d.price_date)                               AS price_date,
  x.price_xdai                                                       AS price_xdai,
  d.price_dai                                                        AS price_dai,
  coalesce(x.price_xdai, d.price_dai, 1.0)                           AS price_usd
FROM xdai x
FULL OUTER JOIN dai d USING (price_date)
ORDER BY price_date