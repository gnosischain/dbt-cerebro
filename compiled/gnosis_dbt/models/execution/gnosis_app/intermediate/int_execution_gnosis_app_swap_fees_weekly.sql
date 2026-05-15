

-- Weekly rollup of int_execution_gnosis_app_swap_fees_daily. Volume is
-- small enough to full-rebuild at the weekly grain.

WITH rolled AS (
    SELECT
        toStartOfWeek(date, 1)              AS week,
        sum(n_filled_swaps)                 AS n_filled_swaps,
        sum(n_distinct_takers)              AS n_distinct_takers_sum_of_daily,
        sum(volume_usd)                     AS volume_usd,
        sum(fee_native_total)               AS fee_native_total,
        sum(fee_usd_total)                  AS fee_usd_total
    FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily`
    WHERE date < toStartOfWeek(today(), 1)
    GROUP BY week
)

SELECT
    week,
    n_filled_swaps,
    n_distinct_takers_sum_of_daily,
    volume_usd,
    fee_native_total,
    fee_usd_total,
    round(fee_usd_total / nullIf(volume_usd, 0) * 100, 4) AS fee_pct_of_volume
FROM rolled
ORDER BY week