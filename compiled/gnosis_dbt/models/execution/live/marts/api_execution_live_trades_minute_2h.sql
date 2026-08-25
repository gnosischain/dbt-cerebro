

-- Minute-grain DEX activity over the last 2h of cached live trades.
-- Mirrors api_execution_live_trades_hourly_48h but at toStartOfMinute.
-- Volume is summed per-swap (per hop); trade_count is swap-event count.
-- USD uses the same daily-price notionals as int_live__dex_trades_raw.

WITH

hwm AS (
    SELECT max(block_timestamp) AS ts
    FROM `dbt`.`int_live__dex_trades_raw`
),

minute AS (
    SELECT
        toStartOfMinute(block_timestamp)        AS date,
        protocol                                AS label,
        count()                                 AS trade_count,
        round(sum(amount_usd), 0)               AS volume_usd
    FROM `dbt`.`int_live__dex_trades_raw` FINAL
    WHERE block_timestamp >= (SELECT ts FROM hwm) - INTERVAL 2 HOUR
      AND block_timestamp <= (SELECT ts FROM hwm) - INTERVAL 60 SECOND
      AND protocol != ''
    GROUP BY date, label
)

SELECT
    date,
    label,
    trade_count,
    volume_usd
FROM minute
ORDER BY date, label