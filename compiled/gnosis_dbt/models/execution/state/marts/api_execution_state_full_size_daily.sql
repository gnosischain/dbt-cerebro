SELECT
    date
    ,bytes/POWER(10,9) AS value
FROM `dbt`.`fct_execution_state_full_size_daily`