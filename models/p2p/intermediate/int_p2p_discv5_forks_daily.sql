{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, label, fork)',
        unique_key='(date, label, fork)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    ) 
}}

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,toString(any(cl_fork_name)) AS fork
        ,toString(any(cl_next_fork_name)) AS next_fork
    FROM {{ ref('int_p2p_discv5_peers') }}
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        {{ apply_monthly_incremental_filter('visit_ended_at', 'date','true') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,'Current Fork' AS label
    ,fork AS fork
    ,COUNT(*) AS cnt
FROM peers
GROUP BY 1, 2, 3

UNION ALL

SELECT
    date
    ,'Next Fork' AS label
    ,next_fork AS fork
    ,COUNT(*) AS cnt
FROM peers
GROUP BY 1, 2, 3