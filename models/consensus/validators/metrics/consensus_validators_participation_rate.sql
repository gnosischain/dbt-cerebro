{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(day)',
        primary_key='(day)',
       partition_by='partition_month'
    ) 
}}

WITH committe_size AS (
    SELECT
        CAST(f_value AS FLOAT) AS f_value
    FROM
        {{ get_postgres('chaind', 't_chain_spec') }}
    WHERE 
        f_key = 'EPOCHS_PER_SYNC_COMMITTEE_PERIOD'
)

SELECT
    toStartOfMonth(toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }})) AS partition_month
    ,toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day
    ,AVG(length(f_indices)/(SELECT f_value FROM committe_size)) AS mean_pct
FROM  
    {{ get_postgres('chaind', 't_sync_aggregates') }} 
{% if is_incremental() %}
WHERE toStartOfMonth(toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }})) >= (SELECT max(partition_month) FROM {{ this }})
{% endif %}
GROUP BY 1, 2

