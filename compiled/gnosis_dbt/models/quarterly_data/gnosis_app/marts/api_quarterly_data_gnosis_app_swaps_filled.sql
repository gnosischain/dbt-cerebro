

SELECT
    toStartOfQuarter(month) AS quarter,
    sum(n_swaps_filled) AS swaps_filled
FROM `dbt`.`fct_execution_gnosis_app_swaps_monthly`
GROUP BY quarter
ORDER BY quarter