{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['day'],
        engine='MergeTree()',
        order_by='day'
    ) 
}}

WITH 

participation AS (
    SELECT
        day,
        distinct_count AS active
    FROM {{ ref('consensus_validators_participation') }}
),

validators AS (
    SELECT 
        day,
        SUM(cnt) OVER (ORDER BY day) AS total_validators
    FROM {{ ref('consensus_validators_activations') }}
)

SELECT 
    p.day AS day,
    p.active,
    v.total_validators,
    CASE 
        WHEN v.total_validators > 0 THEN p.active / v.total_validators
        ELSE NULL
    END AS participation_rate
FROM 
    participation p
INNER JOIN
    validators v
    ON p.day = v.day

