{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date,country)',
        unique_key='(date,country)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

peers_ip AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM {{ ref('p2p_peers_info') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('date','true') }}
    GROUP BY 1, 2
)

SELECT
    t1.date
    ,COALESCE(t2.country,'Unknown') AS country
    ,COUNT(*) AS cnt
FROM peers_ip t1
LEFT JOIN
    {{ source('crawlers_data','ipinfo') }} t2
    ON t1.ip = t2.ip
WHERE t1.date < today()
GROUP BY 1, 2
