

-- Deduped DefiLlama external prices. Raw table is chain-aware; hub grain stays
-- (date, symbol). Prefer gnosis when the same symbol is ingested on multiple
-- chains; otherwise latest ingest wins.

SELECT
    toDate(block_date) AS date,
    upper(symbol) AS symbol,
    argMax(
        lower(token_address),
        (if(lower(chain) = 'gnosis', 1, 0), ingested_at)
    ) AS token_address,
    argMax(
        toFloat64(price),
        (if(lower(chain) = 'gnosis', 1, 0), ingested_at)
    ) AS price,
    argMax(
        confidence,
        (if(lower(chain) = 'gnosis', 1, 0), ingested_at)
    ) AS confidence
FROM `crawlers_data`.`defillama_prices`
GROUP BY date, symbol
ORDER BY date, symbol