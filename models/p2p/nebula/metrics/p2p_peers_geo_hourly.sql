{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date,lat,long)',
        unique_key='(date,lat,long)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    ) 
}}

WITH

peers_ip AS (
    SELECT
        toStartOfHour(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM {{ ref('p2p_peers_info') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at','date','true') }}
    GROUP BY 1, 2
)


SELECT
    date
    ,splitByString(',',loc)[1] AS lat
    ,splitByString(',',loc)[2] AS long
    ,IF(country='','Unknown', country) AS country
    ,cnt
FROM (
    SELECT
        t1.date
        ,t2.loc
        ,t2.country
        ,COUNT(*) AS cnt
    FROM peers_ip t1
    LEFT JOIN
        {{ source('crawlers_data','ipinfo') }} t2
        ON t1.ip = t2.ip
    GROUP BY 1, 2, 3
)