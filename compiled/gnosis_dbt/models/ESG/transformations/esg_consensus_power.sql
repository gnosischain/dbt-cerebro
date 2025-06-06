WITH consensus_power AS (
    SELECT
        type,
        client,
        mean
    FROM (
        SELECT
            arrayJoin([4, 5, 6, 4, 5, 6, 4, 5, 6, 4, 5, 6, 4, 5, 6]) AS type,
            arrayJoin(['Lighthouse', 'Lighthouse', 'Lighthouse', 'Teku', 'Teku', 'Teku', 'Lodestar', 'Lodestar', 'Lodestar', 'Nimbus', 'Nimbus', 'Nimbus', 'Prysm', 'Prysm', 'Prysm']) AS client,
            arrayJoin([2.75, 3.14, 18.84, 3.71, 3.32, 27.46, 3.14, 3.89, 33.55, 1.67, 2.08, 17.11, 3.51, 2.87, 24.33]) AS mean
    )
)

SELECT * FROM consensus_power