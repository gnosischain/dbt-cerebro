

SELECT
    quarter,
    peg_class,
    balance_bucket,
    holders_min,
    holders_max,
    holders_avg,
    holders_median,
    value_min,
    value_max,
    value_avg,
    value_median,
    value_median / nullIf(holders_median, 0) AS avg_balance_usd
FROM `dbt`.`int_quarterly_stablecoin_cohorts_stats`
ORDER BY quarter, peg_class, bucket_order