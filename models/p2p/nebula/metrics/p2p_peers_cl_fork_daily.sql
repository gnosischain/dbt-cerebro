{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, fork)',
        unique_key='(date, fork)',
        partition_by='toStartOfMonth(date)'
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,COALESCE(any(cl_fork_name),'Unknown') AS fork
    FROM {{ ref('p2p_peers_info') }}
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('date','true') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,fork
    ,COUNT(*) AS cnt
FROM peers
WHERE date < today()
GROUP BY 1, 2
