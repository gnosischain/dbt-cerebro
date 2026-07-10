

-- Hourly payment counts by token over the trailing 14 days. Gnosis Chain
-- builds this from raw execution.logs; on Celo the per-transfer int model
-- already carries block_time + a settled Payment classification, so we read it
-- directly (simpler and consistent with our single classification source of
-- truth).
--
-- DENSE by construction: we build a complete hourly spine (14 days x 24h) x the
-- two tracked tokens and left-join the actual counts, zero-filling quiet hours.
-- Without this the series is sparse (only hours that had a payment exist as
-- rows), which turns the chart's category x-axis into irregular, arbitrary tick
-- labels (e.g. "Jun 29, 15:00"). Zero-filling keeps every hour present so ticks
-- land on round hours and gaps read honestly as no-activity. Tokens are pinned
-- to USDC/USDT to match the whitelist scope (a token with zero payments in the
-- window would otherwise be absent entirely).
WITH hours AS (
    SELECT toDateTime(today() - 14) + toIntervalHour(number) AS hour
    FROM numbers(14 * 24)
),

symbols AS (
    SELECT arrayJoin(['USDC', 'USDT']) AS symbol
),

grid AS (
    SELECT h.hour, s.symbol
    FROM hours h
    CROSS JOIN symbols s
),

counts AS (
    SELECT
        toStartOfHour(block_time) AS hour,
        token_symbol              AS symbol,
        count()                   AS payment_count
    FROM `dbt`.`int_celo_gpay_activity`
    WHERE action = 'Payment'
      AND block_time >= today() - 14
      AND block_time <  today()
    GROUP BY hour, symbol
)

SELECT
    g.hour                        AS hour,
    g.symbol                      AS symbol,
    coalesce(c.payment_count, 0)  AS payment_count
FROM grid g
LEFT JOIN counts c
    ON  c.hour   = g.hour
    AND c.symbol = g.symbol
ORDER BY hour, symbol