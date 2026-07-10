

WITH daily AS (
    SELECT
        toStartOfQuarter(date) AS quarter,
        date,
        CASE
            WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
            THEN 'USD-pegged'
            ELSE 'non-USD'
        END AS peg_class,
        sum(holders) AS daily_holders
    FROM `dbt`.`fct_execution_tokens_metrics_daily`
    WHERE token_class = 'STABLECOIN'
      AND symbol NOT IN ('BRZ')
    GROUP BY quarter, date, peg_class
)

, per_class AS (
    SELECT
        quarter,
        peg_class,
        min(daily_holders)    AS holders_min,
        max(daily_holders)    AS holders_max,
        avg(daily_holders)    AS holders_avg,
        median(daily_holders) AS holders_median
    FROM daily
    GROUP BY quarter, peg_class
)

-- Emits the two per-class rows plus a 'total' row that is the column-wise sum of
-- the per-class rows. The total holders_median is the sum of the two per-class
-- medians (matching the quarterly report's Total). Wrapped in a subquery so the
-- trailing ORDER BY applies to the whole UNION, not just its last arm.
SELECT * FROM (
    SELECT
        quarter,
        peg_class,
        holders_min,
        holders_max,
        holders_avg,
        holders_median,
        CASE
            WHEN peg_class = 'USD-pegged' THEN 'WxDAI, sDAI, USDC, USDC.e, USDT'
            ELSE 'EURe, GBPe, BRLA, ZCHF, svZCHF'
        END AS tokens_included
    FROM per_class

    UNION ALL

    SELECT
        quarter,
        'total'             AS peg_class,
        sum(holders_min)    AS holders_min,
        sum(holders_max)    AS holders_max,
        sum(holders_avg)    AS holders_avg,
        sum(holders_median) AS holders_median,
        'ALL (USD-pegged + non-USD, excl BRZ)' AS tokens_included
    FROM per_class
    GROUP BY quarter
)
ORDER BY quarter, peg_class