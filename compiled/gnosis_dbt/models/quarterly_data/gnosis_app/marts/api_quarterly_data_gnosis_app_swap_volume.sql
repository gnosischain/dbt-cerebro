

SELECT
    toStartOfQuarter(month) AS quarter,
    round(sum(volume_usd_filled), 2) AS volume_usd
FROM `dbt`.`fct_execution_gnosis_app_swaps_monthly`
GROUP BY quarter
ORDER BY quarter