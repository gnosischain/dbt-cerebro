

SELECT
    date,
    n_swaps,
    n_swaps_filled,
    n_swaps_unfilled,
    n_swappers,
    n_orders,
    round(toFloat64(volume_usd_filled), 2)  AS volume_usd_filled,
    round(toFloat64(volume_usd_priced), 2)  AS volume_usd_priced,
    n_filled_unpriced
FROM `dbt`.`fct_execution_gnosis_app_swaps_daily`
ORDER BY date