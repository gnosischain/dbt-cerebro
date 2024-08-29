WITH idle_electric_power AS (
    SELECT * FROM (
        VALUES 
            (4, 3.66),
            (5, 25.04),
            (6, 78.17)
    ) AS t(type, mean)
)

SELECT * FROM idle_electric_power