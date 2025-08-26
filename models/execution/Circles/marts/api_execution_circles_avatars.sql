SELECT
    date
    ,avatar_type
    ,cnt
    ,total
FROM {{ ref('fct_execution_circles_avatars') }}
ORDER BY date, avatar_type
