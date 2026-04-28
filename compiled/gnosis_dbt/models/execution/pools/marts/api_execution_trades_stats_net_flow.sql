

-- Top 10 tokens by absolute net USD flow over the last 30 days. Reads from
-- the pre-aggregated daily fact. Subquery isolates aggregates so output
-- aliases don't shadow source columns.

SELECT
    token                       AS label,
    round(net_usd, 0)           AS value,
    round(total_bought, 0)      AS bought_usd,
    round(total_sold, 0)        AS sold_usd
FROM (
    SELECT
        token,
        sum(bought_usd)                         AS total_bought,
        sum(sold_usd)                           AS total_sold,
        sum(bought_usd) - sum(sold_usd)         AS net_usd
    FROM `dbt`.`fct_execution_trades_by_token_daily`
    WHERE date >= today() - INTERVAL 30 DAY
      AND date <  today()
      AND token != ''
    GROUP BY token
    HAVING sum(bought_usd) + sum(sold_usd) > 0
)
ORDER BY abs(net_usd) DESC
LIMIT 10