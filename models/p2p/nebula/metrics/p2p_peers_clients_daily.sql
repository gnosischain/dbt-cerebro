{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(agent_version) AS agent_version
    FROM {{ ref('p2p_peers_info') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('date','true') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,SUM(if(splitByChar('/', agent_version)[1] ='Lighthouse',1,0)) AS Lighthouse
    ,SUM(if(splitByChar('/', agent_version)[1] ='teku',1,0)) AS Teku
    ,SUM(if(splitByChar('/', agent_version)[1] ='lodestar',1,0)) AS Lodestar
    ,SUM(if(splitByChar('/', agent_version)[1] ='nimbus',1,0)) AS Nimbus
    ,SUM(if(splitByChar('/', agent_version)[1] ='erigon',1,0)) AS Erigon
    ,SUM(if(splitByChar('/', agent_version)[1] ='',1,0)) AS Unknown
FROM peers
WHERE date < today()
GROUP BY 1
