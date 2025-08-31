{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, metric, label)',
        unique_key='(date, metric, label)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        },
        tags=['production','p2p','discv4']
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,argMax(IF(client='','Unknown',client),visit_ended_at) AS client
        ,argMax(platform,visit_ended_at) AS platform
        ,argMax(generic_provider,visit_ended_at) AS generic_provider
        ,argMax(peer_country,visit_ended_at) AS peer_country
    FROM {{ ref('int_p2p_discv4_peers') }}
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at','date','true') }}
    GROUP BY 1, 2
)

SELECT date , 'Clients' AS metric, client AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Platform' AS metric, platform AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Provider' AS metric, generic_provider AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Country' AS metric, peer_country AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

