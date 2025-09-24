{{
  config(
    materialized='view',
    tags=['production','execution','transactions']
  )
}}

WITH base AS (
  SELECT
    toDate(block_date) AS price_date,
    upper(symbol)      AS symbol,
    toFloat64(price)   AS price_usd
  FROM {{ source('playground_max','gnosis_daily_bluechip_prices') }}
  WHERE upper(symbol) IN ('XDAI','DAI')
)
SELECT
  price_date,
  maxIf(price_usd, symbol = 'XDAI') AS price_xdai,
  maxIf(price_usd, symbol = 'DAI')  AS price_dai,
  COALESCE(maxIf(price_usd, symbol='XDAI'),
           maxIf(price_usd, symbol='DAI'),
           1.0)                     AS price_usd
FROM base
GROUP BY price_date