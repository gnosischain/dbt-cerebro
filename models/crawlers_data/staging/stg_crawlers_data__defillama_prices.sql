{{
  config(
    materialized='view',
    tags=['production', 'staging', 'crawlers_data', 'prices']
  )
}}

-- Deduped DefiLlama external prices. Append-only raw table; anyLast wins for
-- re-runs on the same (date, symbol) grain (allowlist is one address per symbol).

SELECT
    toDate(block_date)                    AS date,
    upper(symbol)                         AS symbol,
    anyLast(lower(token_address))         AS token_address,
    anyLast(toFloat64(price))             AS price,
    anyLast(confidence)                   AS confidence
FROM {{ source('crawlers_data_external_prices', 'defillama_prices') }}
GROUP BY date, symbol
ORDER BY date, symbol
