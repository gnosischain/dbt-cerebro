WITH idle_electric_power AS (
    SELECT 
        type
        ,mean
    FROM (
        SELECT
            arrayJoin([4, 5, 6]) AS type,
            arrayJoin([3.66, 25.04, 78.17]) AS mean
    )
)

SELECT * FROM idle_electric_power