WITH execution_power AS (
    SELECT * FROM (
        VALUES 
            (4, 'Erigon', 18.6),
            (5, 'Erigon', 17.59),
            (6, 'Erigon', 44.62),
            (4, 'Nethermind', 18.6),
            (5, 'Nethermind', 17.59),
            (6, 'Nethermind', 44.62)
    ) AS t(type, client, mean)
)

SELECT * FROM execution_power