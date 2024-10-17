{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='f_slot'
) }}

WITH postgres_data AS (
    SELECT *
    FROM {{ get_postgres('gnosis_chaind','t_blocks') }}
    LIMIT 100
)

SELECT *
FROM postgres_data
