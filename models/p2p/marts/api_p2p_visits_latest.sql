
{{ 
    config(
        materialized='view',
        tags=['production','p2p','discv4','discv5','visits', 'tier0', 'api: visits_latest']
    ) 
}}

WITH 

discv4_prev7D AS (
    SELECT
        date,
        crawls
    FROM {{ ref('int_p2p_discv4_visits_daily') }}
    WHERE 
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_p2p_discv4_visits_daily') }}), 7)
),

discv4 AS (
    SELECT
        date,
        total_visits,
        ROUND(COALESCE(successful_visits / NULLIF(total_visits, 0), 0) * 100, 1) AS pct_successful,
        crawls
    FROM {{ ref('int_p2p_discv4_visits_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM {{ ref('int_p2p_discv4_visits_daily') }})
),

discv5_prev7D AS (
    SELECT
        date,
        crawls
    FROM {{ ref('int_p2p_discv5_visits_daily') }}
    WHERE 
        date = subtractDays((SELECT MAX(date) FROM {{ ref('int_p2p_discv5_visits_daily') }}), 7)
),

discv5 AS (
    SELECT
        date,
        total_visits,
        ROUND(COALESCE(successful_visits / NULLIF(total_visits, 0), 0) * 100, 1) AS pct_successful,
        crawls
    FROM {{ ref('int_p2p_discv5_visits_daily') }}
    WHERE 
        date = (SELECT MAX(date) FROM {{ ref('int_p2p_discv5_visits_daily') }})
)

SELECT
    t2.total_visits AS discv4_total_visits,
    t2.pct_successful AS discv4_pct_successful,
    t2.crawls AS discv4_crawls,
    ROUND((COALESCE(t2.crawls / NULLIF(t1.crawls, 0), 0) - 1) * 100, 1) AS change_discv4_crawls_pct,
    t4.total_visits AS discv5_total_visits,
    t4.pct_successful AS discv5_pct_successful,
    t4.crawls AS discv5_crawls,
    ROUND((COALESCE(t4.crawls / NULLIF(t3.crawls, 0), 0) - 1) * 100, 1) AS change_discv5_crawls_pct
FROM discv4_prev7D t1
CROSS JOIN discv4 t2
CROSS JOIN discv5_prev7D t3
CROSS JOIN discv5 t4
