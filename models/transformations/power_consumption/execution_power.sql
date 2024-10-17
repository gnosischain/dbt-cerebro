WITH execution_power AS (
    SELECT 
        type,
        client,
        mean
    FROM (
        SELECT
            arrayJoin([4, 5, 6, 4, 5, 6]) AS type,
            arrayJoin(['Erigon', 'Erigon', 'Erigon', 'Nethermind', 'Nethermind', 'Nethermind']) AS client,
            arrayJoin([18.6, 17.59, 44.62, 18.6, 17.59, 44.62]) AS mean
            
    )
)

SELECT * FROM execution_power