{{ config(materialized='incremental', unique_key='last_seen') }}

{% set activity_buffer = '72 hours' %}

WITH 

gnosis_nodes AS (
    SELECT
        enr
        ,timestamp
        ,IF(
            timestamp = max(timestamp) OVER (PARTITION BY enr),
            now(),
            any(timestamp) OVER (
                PARTITION BY enr
                ORDER BY timestamp ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
            )
        ) AS timestamp_lead
    FROM (
        SELECT DISTINCT
            timestamp
            ,enr
        FROM 
            {{ source('valtrack','metadata_received_events') }}
    )
),

nodes_active AS (
    SELECT
        *
		,LEAST(timestamp + INTERVAL '{{ activity_buffer }}', timestamp_lead) AS active_until
    FROM
        gnosis_nodes
),

nodes_status AS (
    SELECT
        *
        ,'active' AS status
    FROM nodes_active
    WHERE active_until = timestamp_lead
    
    UNION ALL
    
    SELECT
        *
        ,'inactive' AS status
    FROM nodes_active
    WHERE active_until < timestamp_lead
)

SELECT * FROM nodes_status