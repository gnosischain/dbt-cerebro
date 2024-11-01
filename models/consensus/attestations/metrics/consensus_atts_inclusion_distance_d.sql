{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day, inc_dist_cohort)',
        primary_key='(day, inc_dist_cohort)',
        partition_by='partition_month'
    ) 
}}

WITH 


inclusion_distance AS (
    SELECT 
         toStartOfMonth({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS partition_month
        ,toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day
        ,f_inclusion_slot - f_slot AS inc_dist_cohort
        ,COUNT(*) AS cnt
    FROM  {{ get_postgres('chaind', 't_attestations') }}
    {% if is_incremental() %}
    WHERE toStartOfMonth({{ compute_timestamp_at_slot('f_inclusion_slot') }}) >= (SELECT max(partition_month) FROM {{ this }})
    {% endif %}
    GROUP BY 1,2,3
)

SELECT * FROM inclusion_distance
