

WITH daily_per_bucket AS (
    SELECT
        toStartOfQuarter(date) AS quarter,
        date,
        CASE
            WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
            THEN 'USD-pegged'
            ELSE 'non-USD'
        END AS peg_class,
        balance_bucket,
        sum(holders_in_bucket)   AS daily_holders,
        sum(value_usd_in_bucket) AS daily_value_usd
    FROM `dbt`.`int_execution_tokens_balance_cohorts_daily`
    WHERE token_class = 'STABLECOIN'
      AND cohort_unit = 'usd'
      AND symbol NOT IN ('BRZ')
    GROUP BY quarter, date, peg_class, balance_bucket
)

SELECT
    quarter,
    peg_class,
    balance_bucket,
    multiIf(
        balance_bucket = '0-0.01',    1,
        balance_bucket = '0.01-0.1',  2,
        balance_bucket = '0.1-1',     3,
        balance_bucket = '1-10',      4,
        balance_bucket = '10-100',    5,
        balance_bucket = '100-1k',    6,
        balance_bucket = '1k-10k',    7,
        balance_bucket = '10k-100k',  8,
        balance_bucket = '100k-1M',   9,
        10
    ) AS bucket_order,
    min(daily_holders)    AS holders_min,
    max(daily_holders)    AS holders_max,
    avg(daily_holders)    AS holders_avg,
    median(daily_holders) AS holders_median,
    min(daily_value_usd)    AS value_min,
    max(daily_value_usd)    AS value_max,
    avg(daily_value_usd)    AS value_avg,
    median(daily_value_usd) AS value_median
FROM daily_per_bucket
GROUP BY quarter, peg_class, balance_bucket, bucket_order
ORDER BY quarter, peg_class, bucket_order