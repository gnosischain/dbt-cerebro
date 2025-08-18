WITH node_distribution AS (
    SELECT 
        type
        ,distribution
    FROM (
        SELECT
            arrayJoin([4, 5, 6]) AS type,
            arrayJoin([0.25, 0.50, 0.25]) AS distribution
    )
)

SELECT * FROM node_distribution