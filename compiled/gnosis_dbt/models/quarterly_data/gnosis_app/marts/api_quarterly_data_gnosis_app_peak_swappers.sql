

SELECT
    toStartOfQuarter(date) AS quarter,
    max(n_swappers) AS peak_daily_swappers,
    argMax(date, n_swappers) AS peak_date
FROM `dbt`.`fct_execution_gnosis_app_swaps_daily`
GROUP BY quarter
ORDER BY quarter