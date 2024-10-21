{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='day'
    ) 
}}

WITH 


inclusion_distance AS (
    SELECT 
        toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day
        ,inc_dist_cohort
        ,SUM(cnt) AS cnt
    FROM {{ ref('consensus_atts_inclusion_distance') }}
    {% if is_incremental() %}
    WHERE toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) >= (SELECT max(day) FROM {{ this }})
    {% endif %}
    GROUP BY 1,2
)

SELECT * FROM inclusion_distance
