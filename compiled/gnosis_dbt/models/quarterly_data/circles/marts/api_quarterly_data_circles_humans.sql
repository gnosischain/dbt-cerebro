

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total, date) AS registered_humans
FROM `dbt`.`fct_execution_circles_v2_avatars`
WHERE avatar_type = 'Human'
  AND date < today()
GROUP BY quarter
ORDER BY quarter