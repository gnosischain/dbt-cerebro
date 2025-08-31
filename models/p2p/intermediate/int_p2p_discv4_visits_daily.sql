{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['production','p2p','discv4']
    ) 
}}

WITH

visits_info AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,COUNT(visit_ended_at) AS total_visits
        ,SUM(IF( empty(dial_errors) = 1 OR crawl_error IS NULL, 1, 0)) AS successful_visits
        ,COUNT(DISTINCT crawl_id) AS crawls
    FROM {{ ref('stg_nebula_discv4__visits') }}
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        toString(peer_properties.network_id) = '100'
        {{ apply_monthly_incremental_filter(source_field='visit_ended_at',destination_field='date',add_and='true') }}
    GROUP BY 1
)

SELECT * FROM visits_info