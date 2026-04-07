

SELECT
    date,
    avatar_type,
    cnt,
    total
FROM `dbt`.`fct_execution_circles_v2_avatars`
WHERE date < today()
ORDER BY date DESC, avatar_type