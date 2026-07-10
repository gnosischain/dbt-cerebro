

WITH daily AS (
    SELECT
        toStartOfQuarter(date) AS quarter,
        date,
        CASE
            WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
            THEN 'USD-pegged'
            ELSE 'non-USD'
        END AS peg_class,
        sum(supply_usd) AS daily_supply_usd,
        sum(holders)    AS daily_holders
    FROM `dbt`.`fct_execution_tokens_metrics_daily`
    WHERE token_class = 'STABLECOIN'
      AND symbol NOT IN ('BRZ')
    GROUP BY quarter, date, peg_class
)

, per_class AS (
    SELECT
        quarter,
        peg_class,
        min(daily_supply_usd)    AS supply_min,
        max(daily_supply_usd)    AS supply_max,
        avg(daily_supply_usd)    AS supply_avg,
        median(daily_supply_usd) AS supply_median
    FROM daily
    GROUP BY quarter, peg_class
)

-- Emits the two per-class rows plus a 'total' row that is the column-wise sum of
-- the per-class rows. The total supply_median is therefore the sum of the two
-- per-class medians (matching the quarterly report's Total), NOT the median of
-- the daily grand-total series (which differs by ~0.8%). Wrapped in a subquery so
-- the trailing ORDER BY applies to the whole UNION, not just its last arm.
SELECT * FROM (
    SELECT
        quarter,
        peg_class,
        supply_min,
        supply_max,
        supply_avg,
        supply_median,
        CASE
            WHEN peg_class = 'USD-pegged' THEN 'WxDAI, sDAI, USDC, USDC.e, USDT'
            ELSE 'EURe, GBPe, BRLA, ZCHF, svZCHF'
        END AS tokens_included
    FROM per_class

    UNION ALL

    SELECT
        quarter,
        'total'             AS peg_class,
        sum(supply_min)     AS supply_min,
        sum(supply_max)     AS supply_max,
        sum(supply_avg)     AS supply_avg,
        sum(supply_median)  AS supply_median,
        'ALL (USD-pegged + non-USD, excl BRZ)' AS tokens_included
    FROM per_class
    GROUP BY quarter
)
ORDER BY quarter, peg_class