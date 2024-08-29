WITH consensus_power AS (
    SELECT * FROM (
        VALUES 
            (4, 'Lighthouse', 2.75),
            (5, 'Lighthouse', 3.14),
            (6, 'Lighthouse', 18.84),
            (4, 'Teku', 3.71),
            (5, 'Teku', 3.32),
            (6, 'Teku', 27.46),
            (4, 'Lodestar', 3.14),
            (5, 'Lodestar', 3.89),
            (6, 'Lodestar', 33.55),
            (4, 'Nimbus', 1.67),
            (5, 'Nimbus', 2.08),
            (6, 'Nimbus', 17.11),
            (4, 'Prysm', 3.51),
            (5, 'Prysm', 2.87),
            (6, 'Prysm', 24.33)
    ) AS t(type, client, mean)
)

SELECT * FROM consensus_power