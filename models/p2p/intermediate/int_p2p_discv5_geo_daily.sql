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
        },
        tags=['production','p2p','discv5']
    ) 
}}

WITH

peers_ip AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM {{ ref('int_p2p_discv5_peers') }}
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at','date','true') }}
    GROUP BY 1, 2
)

SELECT
    t1.date
    ,IF(t2.country='',NULL,splitByString(',',t2.loc)[1]) AS lat
    ,IF(t2.country='',NULL,splitByString(',',t2.loc)[2]) AS long
    ,IF(t2.country='','Unknown', t2.country) AS country
    ,COUNT(*) AS cnt
FROM peers_ip t1
LEFT JOIN
    {{ source('crawlers_data','ipinfo') }} t2
    ON t1.ip = t2.ip
GROUP BY 1, 2, 3, 4
