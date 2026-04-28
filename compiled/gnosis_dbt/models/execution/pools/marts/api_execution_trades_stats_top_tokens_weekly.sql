

-- Weekly token activity, bucketed to top-8 lifetime + 'Other'. Top-8 is
-- picked globally so stack colors stay stable regardless of the dashboard
-- time window. Reads from the pre-aggregated daily fact.

WITH

weekly AS (
    SELECT
        toStartOfWeek(date, 1) AS week,
        token,
        sum(combined_usd)                       AS volume_usd,
        sum(bought_trades + sold_trades)        AS trade_count
    FROM `dbt`.`fct_execution_trades_by_token_daily`
    WHERE date < today()
      AND token != ''
    GROUP BY week, token
),

top_lifetime AS (
    SELECT token
    FROM weekly
    GROUP BY token
    ORDER BY sum(volume_usd) DESC
    LIMIT 8
),

bucketed AS (
    SELECT
        w.week                                                                  AS date,
        if(w.token IN (SELECT token FROM top_lifetime), w.token, 'Other')       AS label,
        sum(w.volume_usd)                                                       AS value_volume,
        sum(w.trade_count)                                                      AS value_trades
    FROM weekly w
    GROUP BY date, label
)

SELECT
    date,
    label,
    value_volume,
    value_trades
FROM bucketed
ORDER BY date, label