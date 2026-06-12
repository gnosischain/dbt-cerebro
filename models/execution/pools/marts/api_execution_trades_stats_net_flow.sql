{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Top 10 tokens by absolute net USD flow, by time window. Each branch ranks
-- independently so the top-10 list is correct per window. Dashboard filters by time_window.

SELECT '1m' AS time_window, label, value, bought_usd, sold_usd
FROM (
    SELECT
        token                                   AS label,
        round(net_usd, 0)                       AS value,
        round(total_bought, 0)                  AS bought_usd,
        round(total_sold, 0)                    AS sold_usd
    FROM (
        SELECT
            token,
            sum(bought_usd)                     AS total_bought,
            sum(sold_usd)                       AS total_sold,
            sum(bought_usd) - sum(sold_usd)     AS net_usd
        FROM {{ ref('fct_execution_trades_by_token_daily') }}
        WHERE date >= today() - INTERVAL 30 DAY
          AND date <  today()
          AND token != ''
        GROUP BY token
        HAVING sum(bought_usd) + sum(sold_usd) > 0
    )
    ORDER BY abs(net_usd) DESC
    LIMIT 10
)

UNION ALL

SELECT '3m' AS time_window, label, value, bought_usd, sold_usd
FROM (
    SELECT
        token                                   AS label,
        round(net_usd, 0)                       AS value,
        round(total_bought, 0)                  AS bought_usd,
        round(total_sold, 0)                    AS sold_usd
    FROM (
        SELECT
            token,
            sum(bought_usd)                     AS total_bought,
            sum(sold_usd)                       AS total_sold,
            sum(bought_usd) - sum(sold_usd)     AS net_usd
        FROM {{ ref('fct_execution_trades_by_token_daily') }}
        WHERE date >= today() - INTERVAL 90 DAY
          AND date <  today()
          AND token != ''
        GROUP BY token
        HAVING sum(bought_usd) + sum(sold_usd) > 0
    )
    ORDER BY abs(net_usd) DESC
    LIMIT 10
)

UNION ALL

SELECT '6m' AS time_window, label, value, bought_usd, sold_usd
FROM (
    SELECT
        token                                   AS label,
        round(net_usd, 0)                       AS value,
        round(total_bought, 0)                  AS bought_usd,
        round(total_sold, 0)                    AS sold_usd
    FROM (
        SELECT
            token,
            sum(bought_usd)                     AS total_bought,
            sum(sold_usd)                       AS total_sold,
            sum(bought_usd) - sum(sold_usd)     AS net_usd
        FROM {{ ref('fct_execution_trades_by_token_daily') }}
        WHERE date >= today() - INTERVAL 180 DAY
          AND date <  today()
          AND token != ''
        GROUP BY token
        HAVING sum(bought_usd) + sum(sold_usd) > 0
    )
    ORDER BY abs(net_usd) DESC
    LIMIT 10
)

UNION ALL

SELECT '1y' AS time_window, label, value, bought_usd, sold_usd
FROM (
    SELECT
        token                                   AS label,
        round(net_usd, 0)                       AS value,
        round(total_bought, 0)                  AS bought_usd,
        round(total_sold, 0)                    AS sold_usd
    FROM (
        SELECT
            token,
            sum(bought_usd)                     AS total_bought,
            sum(sold_usd)                       AS total_sold,
            sum(bought_usd) - sum(sold_usd)     AS net_usd
        FROM {{ ref('fct_execution_trades_by_token_daily') }}
        WHERE date >= today() - INTERVAL 365 DAY
          AND date <  today()
          AND token != ''
        GROUP BY token
        HAVING sum(bought_usd) + sum(sold_usd) > 0
    )
    ORDER BY abs(net_usd) DESC
    LIMIT 10
)
