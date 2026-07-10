

SELECT
    week              AS date,
    cumulative_funded AS value
FROM `dbt`.`fct_celo_gpay_activity_weekly`
ORDER BY date