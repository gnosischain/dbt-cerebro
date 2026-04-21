{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Per-token net USD flow over the last 30 days. A token's "bought" side is
-- net inflow (accumulation); "sold" side is net outflow (distribution).
-- Returns the top-10 tokens by absolute net flow, signed so the dashboard
-- can render as a diverging bar. Not affected by the dashboard window.

WITH

flows AS (
    SELECT
        token_bought_symbol                 AS token,
        sum(coalesce(amount_usd, 0))        AS bought_usd,
        toFloat64(0)                        AS sold_usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE block_timestamp >= today() - INTERVAL 30 DAY
      AND block_timestamp < today()
      AND token_bought_symbol != ''
    GROUP BY token

    UNION ALL

    SELECT
        token_sold_symbol                   AS token,
        toFloat64(0)                        AS bought_usd,
        sum(coalesce(amount_usd, 0))        AS sold_usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE block_timestamp >= today() - INTERVAL 30 DAY
      AND block_timestamp < today()
      AND token_sold_symbol != ''
    GROUP BY token
),

agg AS (
    SELECT
        token,
        sum(bought_usd)                     AS total_bought,
        sum(sold_usd)                       AS total_sold,
        sum(bought_usd) - sum(sold_usd)     AS net_usd
    FROM flows
    GROUP BY token
)

SELECT
    token                                   AS label,
    round(net_usd, 0)                       AS value,
    round(total_bought, 0)                  AS bought_usd,
    round(total_sold, 0)                    AS sold_usd
FROM agg
WHERE total_bought + total_sold > 0
ORDER BY abs(net_usd) DESC
LIMIT 10
