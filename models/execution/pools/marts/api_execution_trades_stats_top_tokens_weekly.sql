{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Weekly token activity, stacked by the top-8 tokens by lifetime volume
-- (sold + bought side combined), with all other tokens rolled into "Other".
-- Top-8 is computed globally so the stack colors stay consistent regardless
-- of the dashboard's time window. The `value_volume` and `value_trades`
-- columns let the dashboard toggle between volume and trade-count views.

WITH

per_token AS (
    -- One row per (hop, side) — a hop contributes amount_usd to BOTH its
    -- sold token and its bought token. Trade count is per unique
    -- (transaction, token) so a multi-hop trade counts once per token
    -- touched.
    SELECT
        toStartOfWeek(block_timestamp, 1)   AS date,  -- mode 1 = Monday start
        transaction_hash,
        token_sold_symbol                   AS token,
        coalesce(amount_usd, 0)             AS usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE amount_usd IS NOT NULL
      AND token_sold_symbol != ''
      AND block_timestamp < today()

    UNION ALL

    SELECT
        toStartOfWeek(block_timestamp, 1)   AS date,
        transaction_hash,
        token_bought_symbol                 AS token,
        coalesce(amount_usd, 0)             AS usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE amount_usd IS NOT NULL
      AND token_bought_symbol != ''
      AND block_timestamp < today()
),

lifetime_rank AS (
    -- Top-8 tokens by lifetime total USD volume.
    SELECT
        token,
        sum(usd) AS lifetime_usd,
        row_number() OVER (ORDER BY sum(usd) DESC) AS rnk
    FROM per_token
    GROUP BY token
),

top_tokens AS (
    SELECT token FROM lifetime_rank WHERE rnk <= 8
),

bucketed AS (
    SELECT
        p.date,
        if(p.token IN (SELECT token FROM top_tokens), p.token, 'Other') AS label,
        p.transaction_hash,
        p.usd
    FROM per_token p
)

SELECT
    date,
    label,
    round(sum(usd), 0)                      AS value_volume,
    uniqExact(transaction_hash)             AS value_trades
FROM bucketed
GROUP BY date, label
ORDER BY date, label
