SELECT
    date
    ,avatar_type
    ,cnt
    ,total
FROM `dbt`.`fct_execution_circles_avatars`
ORDER BY date, avatar_type