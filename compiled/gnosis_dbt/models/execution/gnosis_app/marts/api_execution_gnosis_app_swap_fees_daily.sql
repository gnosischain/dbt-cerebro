

SELECT
    date,
    n_filled_swaps,
    n_distinct_takers,
    volume_usd,
    fee_native_total,
    fee_usd_total,
    fee_pct_of_volume
FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily`
WHERE date < today()
ORDER BY date DESC