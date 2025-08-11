{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, metric, label)',
        unique_key='(date, metric, label)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,argMax(IF(client='','Unknown',client),visit_ended_at) AS client
        ,argMax(
        CASE
            WHEN platform = '' THEN 'Unknown'
            WHEN platform = 'x86_64-linux-gnu' THEN 'linux-x86_64'
            WHEN platform = 'linux-x64' THEN 'linux-x86_64'
            WHEN platform = 'x86_64-unknown-linux-gnu' THEN 'linux-x86_64'
            WHEN platform = 'x86_64-windows' THEN 'windows-x86_64'
            ELSE platform
        END
        ,visit_ended_at) AS platform
    FROM {{ ref('int_p2p_discv4_peers') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at','date','true') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,'Clients' AS metric
    ,client AS label
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT
    date
    ,'Platform' AS metric
    ,platform AS label
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2, 3

