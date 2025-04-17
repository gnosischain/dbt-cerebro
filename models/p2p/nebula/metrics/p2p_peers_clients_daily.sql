{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client)',
        unique_key='(date, client)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(splitByChar('/', agent_version)[1]) AS client
    FROM {{ ref('p2p_peers_info') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at','date','true') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,IF(client='','Unknown',client) AS client
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2
