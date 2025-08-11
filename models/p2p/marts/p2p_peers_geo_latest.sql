{{ config(materialized='view') }}

WITH

peers_ip AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM {{ ref('int_p2p_discv5_peers') }}
    WHERE
        empty(dial_errors) = 1 
        AND 
        crawl_error IS NULL 
        AND 
        date = least((SELECT MAX(toStartOfDay(visit_ended_at)) FROM {{ ref('int_p2p_discv5_peers') }}),today())
    GROUP BY 1, 2
)

SELECT
    splitByString(',',loc)[1] AS lat
    ,splitByString(',',loc)[2] AS long
    ,IF(country='','Unknown', country) AS country
    ,cnt
FROM (
    SELECT
        t2.loc
        ,t2.country
        ,COUNT(*) AS cnt
    FROM peers_ip t1
    LEFT JOIN
        {{ source('crawlers_data','ipinfo') }} t2
        ON t1.ip = t2.ip
    GROUP BY 1, 2
)
