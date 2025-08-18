SELECT
    date
    ,SUM(bytes_diff) OVER (ORDER BY date ASC) AS bytes
FROM `dbt`.`int_execution_state_size_full_diff_daily`
WHERE date < today()