WITH 

discv4_prev7D AS (
    SELECT
        date,
        SUM(value) AS discv4_count
    FROM {{ ref('int_p2p_discv4_clients_daily') }}
    WHERE 
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_p2p_discv4_clients_daily') }}), 7)
        AND metric = 'Clients'
    GROUP BY date
),

discv4 AS (
    SELECT
        date,
        SUM(value) AS discv4_count
    FROM {{ ref('int_p2p_discv4_clients_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM {{ ref('int_p2p_discv4_clients_daily') }})
        AND metric = 'Clients'
    GROUP BY date
),

discv5_prev7D AS (
    SELECT
        date,
        SUM(value) AS discv5_count
    FROM {{ ref('int_p2p_discv5_clients_daily') }}
    WHERE 
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_p2p_discv5_clients_daily') }}), 7)
        AND metric = 'Clients'
    GROUP BY date
),

discv5 AS (
    SELECT
        date,
        SUM(value) AS discv5_count
    FROM {{ ref('int_p2p_discv5_clients_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM {{ ref('int_p2p_discv5_clients_daily') }})
        AND metric = 'Clients'
    GROUP BY date
)

SELECT
    t2.discv4_count AS discv4_count,
    ROUND((COALESCE(t2.discv4_count / NULLIF(t1.discv4_count, 0), 0) - 1) * 100, 1) AS change_discv4_pct,
    t4.discv5_count AS discv5_count,
    ROUND((COALESCE(t4.discv5_count / NULLIF(t3.discv5_count, 0), 0) - 1) * 100, 1) AS change_discv5_pct
FROM discv4_prev7D t1
CROSS JOIN discv4 t2
CROSS JOIN discv5_prev7D t3
CROSS JOIN discv5 t4
