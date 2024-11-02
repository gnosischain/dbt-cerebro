{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day, inc_dist_cohort)',
        unique_key='(day, inc_dist_cohort)',
        partition_by='partition_month'
    ) 
}}

WITH 

{{ get_incremental_filter() }}


inclusion_distance AS (
    SELECT 
        toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day
        ,f_inclusion_slot - f_slot AS inc_dist_cohort
        ,COUNT(*) AS cnt
    FROM  {{ get_postgres('chaind', 't_attestations') }}
    {{ apply_incremental_filter(compute_timestamp_at_slot('f_inclusion_slot')) }}
    GROUP BY 1,2
)

SELECT 
    toStartOfMonth(day) AS partition_month
    ,day
    ,inc_dist_cohort
    ,cnt
FROM 
    inclusion_distance
WHERE
    day < (SELECT MAX(day) FROM inclusion_distance)
