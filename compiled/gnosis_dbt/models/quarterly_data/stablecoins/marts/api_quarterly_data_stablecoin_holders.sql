

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

SELECT
    quarter,
    peg_class,
    min(daily_holders)    AS holders_min,
    max(daily_holders)    AS holders_max,
    avg(daily_holders)    AS holders_avg,
    median(daily_holders) AS holders_median,
    CASE
        WHEN peg_class = 'USD-pegged' THEN 'WxDAI, sDAI, USDC, USDC.e, USDT'
        ELSE 'EURe, GBPe, BRLA, ZCHF, svZCHF'
    END AS tokens_included
FROM daily
GROUP BY quarter, peg_class
ORDER BY quarter, peg_class