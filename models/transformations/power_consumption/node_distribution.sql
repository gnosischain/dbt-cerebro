WITH node_distribution AS (
    SELECT * FROM (
        VALUES 
            (4, 0.25),
            (5, 0.50),
            (6, 0.25)
    ) AS t(type, distribution)
)

SELECT * FROM node_distribution