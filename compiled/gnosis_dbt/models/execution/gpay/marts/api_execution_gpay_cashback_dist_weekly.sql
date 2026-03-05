

SELECT
    week AS date,
    q05, q10, q25, q50, q75, q90, q95,
    average
FROM `dbt`.`fct_execution_gpay_cashback_dist_weekly`
WHERE unit = 'native'
ORDER BY date