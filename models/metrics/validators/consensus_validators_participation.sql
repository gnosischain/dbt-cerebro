{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['day'],
        engine='MergeTree()',
        order_by='day'
    ) 
}}

WITH chunked_data AS (
    SELECT
        toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) AS day,
        f_aggregation_indices,
        intDiv(rowNumberInAllBlocks(), 10000) AS chunk
    FROM {{ get_postgres('gnosis_chaind', 't_attestations') }}
    {% if is_incremental() %}
    WHERE toDate({{ compute_timestamp_at_slot('f_inclusion_slot') }}) >= (SELECT max(day) FROM {{ this }})
    {% endif %}
),
chunked_aggregation AS (
    SELECT
        day,
        chunk,
        arrayDistinct(arrayFlatten(groupArrayArray(f_aggregation_indices))) AS chunk_distinct_array
    FROM chunked_data
    GROUP BY day, chunk
)

SELECT
    day,
    length(arrayDistinct(arrayFlatten(groupArrayArray(chunk_distinct_array)))) AS distinct_count
FROM chunked_aggregation
GROUP BY day