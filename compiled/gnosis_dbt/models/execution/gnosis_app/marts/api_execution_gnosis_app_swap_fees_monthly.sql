

SELECT
    month,
    n_filled_swaps,
    volume_usd,
    fee_native_total,
    fee_usd_total,
    fee_pct_of_volume
FROM `dbt`.`int_execution_gnosis_app_swap_fees_monthly`
ORDER BY month DESC