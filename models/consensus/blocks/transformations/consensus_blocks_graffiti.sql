{{ 
   config(
       materialized='incremental',
       incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day, graffiti, f_proposer_index)',
        primary_key='(day, graffiti, f_proposer_index)',
       partition_by='partition_month'
   ) 
}}


WITH blocks_graffiti AS (
    SELECT
        toStartOfMonth({{ compute_timestamp_at_slot('f_slot') }}) AS partition_month
        ,toDate({{ compute_timestamp_at_slot('f_slot') }}) AS day
        ,f_proposer_index
        ,{{ decode_graffiti('f_graffiti') }} AS graffiti
        ,COUNT(*) AS cnt
    FROM
       {{ get_postgres('chaind', 't_blocks') }}
       {% if is_incremental() %}
        WHERE toStartOfMonth({{ compute_timestamp_at_slot('f_slot') }}) >= (SELECT max(partition_month) FROM {{ this }})
       {% endif %}
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM blocks_graffiti
