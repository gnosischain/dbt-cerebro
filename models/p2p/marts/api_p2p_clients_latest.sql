WITH 

discv4 AS (
    SELECT
        SUM(value) AS discv4_count
    FROM {{ ref('int_p2p_discv4_clients_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM  {{ ref('int_p2p_discv4_clients_daily') }} )
        AND metric = 'Clients'
),

discv5 AS (
    SELECT
        SUM(value) AS discv5_count
    FROM {{ ref('int_p2p_discv5_clients_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM  {{ ref('int_p2p_discv5_clients_daily') }} )
        AND metric = 'Clients'
)

SELECT
   discv4_count 
   ,discv5_count
FROM
    discv4
CROSS JOIN  
    discv5