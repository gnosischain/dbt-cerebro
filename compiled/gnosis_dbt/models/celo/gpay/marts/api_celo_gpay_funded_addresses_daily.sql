

SELECT
    date              AS date,
    cumulative_funded AS value
FROM `dbt`.`fct_celo_gpay_activity_daily`
ORDER BY date