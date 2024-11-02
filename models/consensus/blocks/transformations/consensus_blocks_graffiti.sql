{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day, graffiti, f_proposer_index)',
        unique_key='(day, graffiti, f_proposer_index)',
        partition_by='partition_month'
    ) 
}}

WITH 

{{ get_incremental_filter() }}

blocks_graffiti AS (
    SELECT
        toDate({{ compute_timestamp_at_slot('f_slot') }}) AS day
        ,f_proposer_index
        ,{{ decode_graffiti('f_graffiti') }} AS graffiti
        ,COUNT(*) AS cnt
    FROM
       {{ get_postgres('chaind', 't_blocks') }}
    {{ apply_incremental_filter(compute_timestamp_at_slot('f_slot')) }}
    GROUP BY 1, 2, 3
)

SELECT 
    toStartOfMonth(day) AS partition_month
    ,day
    ,f_proposer_index
    ,graffiti
    ,cnt
FROM 
    blocks_graffiti
WHERE
    day < (SELECT MAX(day) FROM blocks_graffiti)
