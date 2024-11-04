{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day)',
        unique_key='(day)',
        partition_by='partition_month'
    ) 
}}

WITH 

{{ get_incremental_filter() }}

committe_size AS (
    SELECT
        CAST(f_value AS FLOAT) AS f_value
    FROM
        {{ get_postgres('chaind', 't_chain_spec') }}
    WHERE 
        f_key = 'EPOCHS_PER_SYNC_COMMITTEE_PERIOD'
),

final AS (
    SELECT
        toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day
        ,AVG(length(f_indices)/(SELECT f_value FROM committe_size)) AS mean_pct
    FROM  
        {{ get_postgres('chaind', 't_sync_aggregates') }} 
    {{ apply_incremental_filter(compute_timestamp_at_slot('f_inclusion_slot')) }}
    GROUP BY 1
)

SELECT
    toStartOfMonth(day) AS partition_month
    ,day
    ,mean_pct
FROM
    final
WHERE
    day < (SELECT MAX(day) FROM final)

